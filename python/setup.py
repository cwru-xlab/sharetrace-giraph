import setuptools

setuptools.setup(
	name='sharetrace',
	version='1.0',
	packages=setuptools.find_packages(),
	package_dir='sharetrace',
	url='https://github.com/rtatton/ShareTrace',
	license='',
	author='Ryan Tatton',
	author_email='ryan.tatton@case.edu',
	description='The computing backend for the ShareTrace mobile application.',
	python_requires='>= 3.8',
)
