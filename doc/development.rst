

Development
===========

| Welcome to contributing to ``pgmq-sqlalchemy`` !  
| This document will guide you through the process of contributing to the project.

How to Contribute
-----------------

1. Fork the repository
   - Click the `Fork` button in the upper right corner of the repository page.
2. Clone the repository
   - Clone the repository to your local machine.

   .. code-block:: bash

      git clone https://github.com/your-username/pgmq-sqlalchemy.git

3. Create a new branch
   - Create a new branch for your changes.

   .. code-block:: bash

      git checkout -b feature/your-feature-name

4. Make your changes
   - Make your changes to the codebase.
   - Add tests for your changes.
   - Add documentation if changes are user-facing.
5. Commit your changes
    * Commit your changes with meaningful commit messages.
        * `ref: conventional git commit messages <https://www.conventionalcommits.org/en/v1.0.0/>`_

   .. code-block:: bash

      git commit -m "feat: your feature description"

6. Push your changes
   - Push your changes to your forked repository.

   .. code-block:: bash

      git push origin feature/your-feature-name

7. Create a Pull Request
   - Create a Pull Request from your forked repository to the ``develop`` branch of the original repository.

Development Setup
-----------------

Setup
~~~~~

Install dependencies and `ruff` pre-commit hooks.

.. code-block:: bash

   make install

Prerequisites: **Docker** and **Docker Compose** installed.

Start development PostgreSQL

.. code-block:: bash

   make start-db

Stop development PostgreSQL

.. code-block:: bash

   make stop-db

Makefile utility
~~~~~~~~~~~~~~~~

.. code-block:: bash

   make help

   # will show all available commands and their descriptions.

Linting
~~~~~~~

We use `pre-commit <https://pre-commit.com/>`_ hook with `ruff <https://github.com/astral-sh/ruff-pre-commit>`_ to automatically lint the codebase before committing.

Testing
-------

Run tests locally

.. code-block:: bash

   make test-local

Run tests in docker

.. code-block:: bash

   make test-docker

Documentation
-------------

Serve documentation

.. code-block:: bash

   make doc-serve

Clean documentation build

.. code-block:: bash

   make doc-clean
