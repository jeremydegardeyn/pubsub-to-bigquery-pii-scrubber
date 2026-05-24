from setuptools import find_packages, setup

setup(
    name="pubsub-to-bigquery-pii-scrubber",
    version="1.0.0",
    description="Dataflow streaming pipeline: Pub/Sub JSON → PII scrub → BigQuery",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.9",
    install_requires=[
        "apache-beam[gcp]>=2.55.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
        ]
    },
)
