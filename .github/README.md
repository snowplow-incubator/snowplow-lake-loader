# rcplus-alloy-snowplow-lake-loader

The Snowplow lake loader reads the stream of enriched events from a Kinesis Data Stream and writes them to S3.

Forked and adapted from the [original Snowplow lake loader](https://github.com/snowplow-incubator/snowplow-lake-loader).

Current version: **v0.1.0**

_**NOTE**_: this README.md is placed in the `.github` folder to avoid the duplication of the same content in the main README.md.
`/.github/README.md` generally has higher priority than `/README.md` to be displayed as the default page at github.com.

## Development, testing, and release management

Several constraints made the local development process not so easy:
1. Because Snowplow uses the sbt native packaging capabilities to produce and publish the Docker images,
   there is no official repeatable way to do the development and tests in a containerized environment.
2. The Scala ecosystem does not have an official dev tool although `sbt` seems to be the most popular one.
   But `sbt` does not have a good support for Dockerized development.
3. This lake loader application heavily uses the Spark/Hadoop standard libraries,
   which sit on an ancient (8+ years obsoleted) toolchain.
   Therefore, the application has to be built with Java 11 to avoid massively tweaking the properties in project config. 

All in all, we create a separate development Docker image for debugging and testing purposes.

To build the development Docker image, run the following command:
```bash
docker build -t lakeloader -f ./Dockerfile.alloy-dev .  
```

For debugging and testing, run the following command:
```bash
docker run --rm -it -v $PWD:/lake-loader -v /var/run/docker.sock:/var/run/docker.sock lakeloader /bin/bash

# and run the test suite in the container
cd /lake-loader
sbt test
```

## Regular maintenance

Regularly check the upstream repo at https://github.com/snowplow-incubator/snowplow-lake-loader.git,
if there is any new release or update, review the changes and decide if this repo should be updated.

In the case of yes,
1. create a branch of this repo from `origin/main`
2. merge the upstream changes into the branch
3. if the changes alter the build environment,
   update `Dockerfile.alloy-dev` to align the Scala version with `./project/BuildSettings` and the sbt version with `./project/build.properties` 
4. test the changes
5. if everything is fine, create a PR to merge the changes into `main`
6. bump the version of main using `bumpsemver`
7. push all the changes and the new tag to the origin

## NOTE

This repo has a redacted GitHub Actions workflow for CI.

It tests all the PRs, build and push a Docker image (to DockerHub as a public image) if the git commit is tagged.
In case the tag starts with `v`, it tags the image additionally as `latest`.
