from setuptools import setup, find_packages

# Read dependencies from requirements.txt
with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="my_package",
    version="0.1",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'my_package=code.main:main',
        ],
    },
    install_requires=required,
    description="A Python package with PySpark integration",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Sai Teja",
    author_email="saitejaranga1@gmail.com",
    url="https://github.com/saitejaranga/ace_case_study",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
