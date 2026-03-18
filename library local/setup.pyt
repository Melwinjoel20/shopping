from setuptools import setup, find_packages
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()
setup(
    name="easycart-rate-limiter",
    version="0.2.0",
    author="Melwin Joel Pinto",
    author_email="melwinpintoir@gmail.com",
    description="A rate limiter package with DynamoDB backend support",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Melwinjoel20/CPP/tree/main/EasyCart/easycart_rate_limiter",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],

    python_requires=">=3.7",

    install_requires=[
        "boto3>=1.26.0",
    ],

)
