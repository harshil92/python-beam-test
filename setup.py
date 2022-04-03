import setuptools
REQUIRED_PACKAGES = []

PACKAGE_NAME = 'virgin_data_eng_test'
PACKAGE_VERSION = '0.0.1'

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='This pipeline processes transaction data',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    package_data= {
        PACKAGE_NAME: [],
    },
)