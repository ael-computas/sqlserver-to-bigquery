import setuptools

with open("package_readme.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='database-to-bigquery',
    version='1.0.0',
    description='Read from SQL server and load to Google BigQuery',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Anders Elton',
    url='https://github.com/ael-computas/sqlserver-to-bigquery',
    download_url='https://github.com/ael-computas/sqlserver-to-bigquery',
    keywords=['gcp', 'BigQuery', 'SQL Server', 'integration', 'copy'],
    package_data={'': ['data/*.json']},
    install_requires=['google-cloud-storage>=1.37.1',
                      'google-cloud-bigquery>=2.13.1',
                      'smart_open[gcs]>=5.0.0',
                      'pyodbc>=4.0.30',
                      'sqlalchemy>=1.4.10',
                      'backoff>=1.10.0',
                      'google-cloud-secret-manager>=2.4.0',
                      'PyYAML>=5.4.1'],
    packages=setuptools.find_packages(),
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
