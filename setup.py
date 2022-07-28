from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="src",
    version="0.0.2",
    author="sid",
    description="A small pyspark-app",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/entbappy/DVC-ML-DEMO",
    author_email="shuklasid19@gmail.com",
    packages=["src"],
    python_requires=">=3.7",
)