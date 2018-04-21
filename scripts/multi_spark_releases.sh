#!/usr/bin/env bash

# link to the spark-daria GitHub release script: https://github.com/MrPowers/spark-daria/blob/master/scripts/github_release.sh
# need to clone the spark-daria repo and feed the release script as an argument to this script

SPARK_DARIA_GITHUB_RELEASE=$1
if [ "$SPARK_DARIA_GITHUB_RELEASE" = "" ]
  then
    echo "spark-daria github_release script path must be set"
    exit 1
fi

for sparkVersion in 2.2.0 2.2.1 2.3.0; do
  echo $sparkVersion
  sed -i '' "s/^val sparkVersion.*/val sparkVersion = \"$sparkVersion\"/" build.sbt
  $SPARK_DARIA_GITHUB_RELEASE package
done