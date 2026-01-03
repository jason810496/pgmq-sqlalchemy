"""FastAPI API server for Order management with PGMQ message publishing.

This example demonstrates:
- Using FastAPI with SQLAlchemy sync session (psycopg2)
- Publishing messages to PGMQ using PGMQOperation (op)
- Creating orders and sending them to a message queue
"""
from typing import Generator
from contextlib import contextmanager, asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, ConfigDict
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import Session, sessionmaker, declarative_base
from datetime import datetime

from pgmq_sqlalchemy import op

# Database configuration
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
QUEUE_NAME = "order_queue"

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


# Order Model (SQLAlchemy ORM)
class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    customer_name = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# Pydantic models for request/response
class OrderCreate(BaseModel):
    customer_name: str
    product_name: str
    quantity: int
    price: float


class OrderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    customer_name: str
    product_name: str
    quantity: int
    price: float
    created_at: datetime
    message_id: int


# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database tables and PGMQ queue on startup."""
    # Startup
    Base.metadata.create_all(bind=engine)
    
    # Initialize PGMQ queue
    with SessionLocal() as session:
        op.check_pgmq_ext(session=session, commit=True)
        
        # Create queue if it doesn't exist
        try:
            op.create_queue(QUEUE_NAME, session=session, commit=True)
        except Exception:
            # Queue might already exist, which is fine
            pass
    
    yield
    
    # Shutdown (if needed)


# FastAPI app with lifespan
app = FastAPI(title="Order Management with PGMQ", lifespan=lifespan)

# Database dependency
def get_db() -> Generator[Session, None, None]:
    """Database session dependency."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/orders", response_model=OrderResponse, status_code=201)
def create_order(order_data: OrderCreate, db: Session = Depends(get_db)):
    """Create a new order and publish it to the message queue.
    
    Args:
        order_data: Order information
        db: Database session
        
    Returns:
        Created order with message ID
    """
    # Create order in database
    db_order = Order(
        customer_name=order_data.customer_name,
        product_name=order_data.product_name,
        quantity=order_data.quantity,
        price=order_data.price,
    )
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    
    # Publish message to PGMQ using op
    message_data = {
        "order_id": db_order.id,
        "customer_name": db_order.customer_name,
        "product_name": db_order.product_name,
        "quantity": db_order.quantity,
        "price": db_order.price,
        "created_at": db_order.created_at.isoformat(),
    }
    
    msg_id = op.send(QUEUE_NAME, message_data, session=db, commit=True)
    
    # Return order with message ID
    return OrderResponse(
        id=db_order.id,
        customer_name=db_order.customer_name,
        product_name=db_order.product_name,
        quantity=db_order.quantity,
        price=db_order.price,
        created_at=db_order.created_at,
        message_id=msg_id,
    )


@app.get("/orders/{order_id}", response_model=OrderCreate)
def get_order(order_id: int, db: Session = Depends(get_db)):
    """Get order by ID.
    
    Args:
        order_id: Order ID
        db: Database session
        
    Returns:
        Order information
    """
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
