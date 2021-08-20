# Montoux Athena Library

This Python library is a simple wrapper around boto3, to provide easier access to AWS Athena.
In this library you can inspect databases and tables that have been created in Athena and perform simple queries to
return AWS S3 URIs for results, or load them directly into a Pandas dataframe.

It's very much work in progress.

## Documentation

The rendered Sphinx documentation can be found
[here](https://git.montoux.com/pages/montoux/montoux_athena/).

## Releases

The version is defined in the file `VERSION`. Please consider the conventions outlined in PEP440
[Example Version Schemes](https://www.python.org/dev/peps/pep-0440/#examples-of-compliant-version-schemes).

Each commit to the repository will build a new package (`whl`) and the documentation.
These are available from the respective [Continuous Integration jobs](
https://git.montoux.com/montoux/montoux_athena/-/jobs) which provide the artifacts as downloadable archives (see
"download" buttons on the right).

Set a commit tag with `release/X.Y.Z` in order to "release" (i.e. upload to AWS codeartifact's pypi index
and publish the documentation), The upload will fail if the package version already exists in the index.

Please refer to the [delete dackage command](https://docs.aws.amazon.com/codeartifact/latest/ug/delete-package.html)
documentation for instructions how to remove a bogous upload.
