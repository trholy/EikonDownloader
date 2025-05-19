from setuptools import setup, find_packages

setup(
    name="eikondownloader",
    version="0.1.0",
    author="Thomas R. Holy",
    author_email="thomas.robert.holy@gmail.com",
    description="Download data from Refinitiv Eikon",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/trholy/eikondownloader",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    license_files=('LICENSE',),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "minio>=7.0.0",
        "numpy>=1.24.3",
        "pandas>=1.5.2",
        "eikon>=1.1.18",
    ],
    extras_require={
        "dev": [
            "pytest",
            "ruff"
        ]
    },
    test_suite='pytest',
)
