from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='lucidsparkutils',
    version='1.0',
    packages=find_packages(),
    description='Utility functions for working with Spark DataFrames and Delta tables.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='William Crayger',
    author_email='wcrayger@lucidbi.co',
    url='https://github.com/lucid-will/lucid-spark-utils',
    license='Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License',
    install_requires=requirements,
    python_requires='>=3.6',
)