# Initialise new build stage and set the base image
FROM python:3.11-slim AS base

# Prevent Python from writing .pyc files when importing modules
# Saves on disk space, keeps container immutable, 
# and prevent .pyc write access to filesystem (minor performance boost)
ENV PYTHONDONTWRITEBYTECODE=1

# Set locale and character encoding
ENV LANG C.UTF-8

# Full locale override
ENV LC_ALL C.UTF-8

# Prevent Python from buffering stdout and stderr
# Better viewing of logs in real-time
ENV PYTHONUNBUFFERED=1

# Turn on Python fault handler to print stack trace if system crashes
ENV PYTHONFAULTHANDLER=1


# Initialise new build stage based on base image to setup Python environment
FROM base AS python-deps

# Update and install Linux packages
RUN apt-get update
# && apt-get install -y --no-install-recommends

# Install pip requirements
RUN pip install pipenv
COPY Pipfile .
COPY Pipfile.lock .

# Create virtual environment inside the project directory and sync with Pipfile.lock
RUN PIPENV_VENV_IN_PROJECT=1 pipenv sync

# Initialise new build stage based on base image to setup runtime environment
FROM base AS runtime

# Copy virtualenv from python-deps stage to runtime stage
COPY --from=python-deps /.venv /.venv

# Add virtualenv's bin directory to PATH variable
# So default python and pip commands are executed from the virtualenv and not from global Python installation
ENV PATH="/.venv/bin:$PATH"

# Create and switch to a new user
RUN useradd --create-home appuser
WORKDIR /home/appuser
USER appuser

# Add application to container
COPY . .

# Run application as a module
ENTRYPOINT ["python", "-m", "src"]
