# batch_processing_IaC_AWS
This repository includes an Infrastructure as Code (IaC) to perform batch processing of big data.

## Installing and Running LocalStack

LocalStack provides an easy-to-use test and development environment for AWS cloud resources on your local machine. In this guide, you'll help how to install and run LocalStack.

### Prerequisites

Before you begin, make sure you have the following dependencies installed on your system:

1. **Python** (check with `python --version`)
2. **pip** (check with `pip --version`)
3. **Docker** (check with `docker --version`)

If you don't have Docker installed, follow the installation instructions for your platform at [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/).

### Installation

1. Install LocalStack using pip:

```
pip install localstack
```

### Running LocalStack

1. Start LocalStack with Docker:

```
localstack start
```

This command downloads the LocalStack Docker image (if not already present on your system) and starts a new LocalStack container with the default services.

After installing and starting LocalStack, you can use and test various AWS services provided by LocalStack.

