from setuptools import setup, find_packages

setup(
    name="db-autofill",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "psycopg2-binary>=2.9.9",
        "faker>=25.4.0",
        "click>=8.1.7",
        "pydantic>=2.8.2",
    ],
    entry_points={
        "console_scripts": [
            "autofill=src.autofill:main",
        ],
    },
    python_requires=">=3.10",
)
