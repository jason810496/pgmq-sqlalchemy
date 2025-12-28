FROM python:3.12-bookworm as python-base

ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # pip
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    # paths
    # this is where our requirements + virtual environment will live
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"


# prepend venv to path
ENV PATH="$VENV_PATH/bin:$PATH"

FROM python-base as builder-base
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        # deps for installing uv
        curl \
        # deps for building python deps
        build-essential

# install uv
RUN --mount=type=cache,target=/root/.cache \
    curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"

# copy project requirement files here to ensure they will be cached.
WORKDIR $PYSETUP_PATH
COPY pyproject.toml uv.lock ./

# install runtime deps
RUN --mount=type=cache,target=/root/.cache \
    uv sync --extra dev

FROM python:3.12-slim-bookworm as runtime

ENV VENV_PATH="/opt/pysetup/.venv"
ENV PATH="$VENV_PATH/bin:$PATH"

COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH
COPY ./pgmq_sqlalchemy /pgmq_sqlalchemy_test/pgmq_sqlalchemy
COPY ./tests /pgmq_sqlalchemy_test/tests

WORKDIR /pgmq_sqlalchemy_test

CMD ["python", "-m", "pytest", "-sv" , "tests", "--cov=pgmq_sqlalchemy.queue", "-n" , "4" ]