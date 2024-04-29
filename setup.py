from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='lucid-control-framework',
    version='0.1',
    packages=find_packages(),
    description='Lucid Control Framework',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='William Crayger',
    author_email='wcrayger@lucidbi.co',
    url='https://github.com/yourusername/lucid-control-framework',
    license='All Rights Reserved',
    classifiers=[
        'License :: Other/Proprietary License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    install_requires=requirements,
    python_requires='>=3.6',
)