# Nurion

A modern data platform workspace combining orchestration and multimodal data processing capabilities.

## Name Origins

**Nurion** draws its name from Norse mythology, representing the god of light and wisdom. In the realm of data platforms, we strive to extract insights from complex data, illuminating the path to better decisions. This name embodies our pursuit of intelligent data processing and analysis.

## Project Purpose

Nurion is a modern data platform workspace designed to provide:

- **Data Orchestration & Coordination**: Task management, Kubernetes integration, and data lake catalog APIs through the Aether service
- **Multimodal Data Processing**: Support for Ray, Spark, and other compute modes through the Solstice framework
- **Unified Development Experience**: Consistent development environment and toolchain
- **Scalable Architecture**: Microservices architecture and containerized deployment support

### Core Components

- **Aether**: FastAPI-driven orchestration service connecting tasks, infrastructure, and data products
- **Solstice**: Ray and Spark-based multimodal data processing toolkit

## Development Setup

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager
- Docker and Docker Compose (for integration testing)

### Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd nurion
   ```

2. **Create virtual environment**
   ```bash
   uv venv
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   uv sync
   ```

4. **Run development services**
   ```bash
   # Start Aether API service
   cd aether
   uv run uvicorn aether.app:app --reload
   ```

### Development Tools

The project provides convenient development scripts:

```bash
# Run complete CI checks
./scripts/ci.sh

# Run code quality checks only
./scripts/lint.sh

# Run unit tests only
./scripts/test.sh
```

### Project Structure

```
nurion/
├── aether/          # Orchestration service (FastAPI)
├── solstice/        # Data processing toolkit (Ray/Spark)
├── infra/           # Pulumi infrastructure (K8s deployment)
├── e2e/             # End-to-end test suite
├── scripts/         # Development and CI scripts
└── pyproject.toml   # Workspace configuration
```

### Development Standards

- **Code Style**: Ruff for code formatting and quality checks
- **Testing**: pytest for unit testing with coverage reporting
- **Commit Convention**: Follow [Conventional Commits](https://conventionalcommits.org/) specification
- **CI/CD**: GitHub Actions for continuous integration

### Detailed Documentation

- [Aether Service Documentation](aether/README.md) - Detailed orchestration service documentation
- [Solstice Framework Documentation](solstice/README.md) - Detailed data processing toolkit documentation
- [Nightly E2E Testing Setup](e2e/README.md) - E2E testing infrastructure and configuration

## E2E Testing

Nurion includes a comprehensive end-to-end testing suite that runs nightly on Volcengine Kubernetes:

```bash
# Run E2E tests locally
cd e2e
uv sync
uv run pytest -v -m "e2e"

# Deploy infrastructure with Pulumi
cd infra
uv sync
pulumi up -s nightly
```

See [Nightly E2E Testing Setup](e2e/README.md) for detailed configuration.

## Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feat/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feat/amazing-feature`)
5. Create a Pull Request

Please ensure all commits follow the Conventional Commits specification and pass all CI checks.
