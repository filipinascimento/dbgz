import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dbgz",
    version="0.2.6",
    author="Filipi N. Silva",
    author_email="filipi@iu.edu",
    description="Python library to load DBGZ files",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/filipinascimento/dbgz",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)