# Ad Service

The Ad service provides advertisement based on context keys. If no context keys are provided then it returns random ads.

## NATS page-view processing

The `ad-page-views-v1` durable processes page views in batches of 32 with eight
concurrent handlers by default. Within each batch it retains only the newest
view per session. Views older than `AD_PAGE_VIEW_MAX_AGE` (default `5s`) and
superseded views are acknowledged without generating obsolete ad selections.
Malformed events retain the normal retry behavior.

Retained inputs are decoded once, and their deterministic ad-selection event is
confirmed by JetStream before the input is acknowledged. Configure batch and
worker counts with `NATS_CONSUMER_BATCH_SIZE` and
`NATS_CONSUMER_CONCURRENCY`. CPU requests and limits are independent of these
settings.

## Building locally

The Ad service uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Ad Service, run:

```
./gradlew installDist
```
It will create executable script src/adservice/build/install/hipstershop/bin/AdService

### Upgrading gradle version
If you need to upgrade the version of gradle then run

```
./gradlew wrapper --gradle-version <new-version>
```

## Building docker image

From `src/adservice/`, run:

```
docker build ./
```
