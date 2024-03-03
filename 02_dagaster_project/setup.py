from setuptools import find_packages, setup

setup(
    name="investment_dagster",
    packages=find_packages(exclude=["investment_dagster_tests"]),
    install_requires=[
        "dagster==1.5.9",
        "dagster-cloud",
        "pandas",
        "plotly",
        "shapely",
        "influxdb-client"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
