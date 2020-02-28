import setuptools

readme_file = open("README.md", "r+")
long_description = readme_file.read()

setuptools.setup(
    name="pyspark-expectations",
    version="0.0.1",
    author="Tatsiana Mazouka",
    author_email="tatsian.mazouka@dm.de",
    description="Data Validation Package for PySpark DataFrames",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.dm-drogeriemarkt.com/cxa/cxa-common/great-expectations.git",
    packages=setuptools.find_packages(),
    install_requires=["pyspark"],
    classifiers=[
        'Programming Language :: Python :: 3.7',
        "Operating System :: OS Independent" 
    ],
)