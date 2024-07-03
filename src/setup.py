import setuptools

setuptools.setup(
    name='migration_pipeline',
    version='0.0.1',
    description='Migration pipeline set workflow package.',
    install_requires=['PyYAML==6.0', 'pandas==1.5.3', 'db-dtypes==1.1.1', 'google-cloud-bigquery',
                      'google-cloud-storage==2.9.0', 'google-cloud-pubsub==2.16.0',
                      'google-cloud-secret-manager==1.0.2'],
    packages=setuptools.find_packages()
)