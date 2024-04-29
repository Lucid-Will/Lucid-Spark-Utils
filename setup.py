from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='lucid_control_framework',
    version='0.2',
    packages=find_packages(),
    description='Lucid Control Framework',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='William Crayger',
    author_email='wcrayger@lucidbi.co',
    url='https://github.com/lucid-will/lucid-control-framework',
    license='All Rights Reserved',
    install_requires=requirements,
    python_requires='>=3.6',
)