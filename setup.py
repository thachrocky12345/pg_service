from setuptools import find_packages, setup

setup(
    name='elecsys_postgres_database',
    version='1.1.0',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    package_data={'': ['']},
    install_requires=[

        'pandas'
    ],
    description='Python library containing logic specific to copy data, bulk insert, '
                'pandas machine learning.'
)
