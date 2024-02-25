# Metabase Automation

## Introduction

This software is part Final Degree Project created by Pelayo Campa González-Nuevo. Is intended to copy metabase dashboards between different instances.

# How to use

This script is working on Python 3. You should install dependencies using:

`pip install -r requirements.txt`

Then, provide a `.env` file in the same folder you'll run the script. This file should be similar to:

```
SOURCE_INSTANCE_URL=
SOURCE_INSTANCE_USER=
SOURCE_INSTANCE_PASSWORD=

DEST_INSTANCE_URL=
DEST_INSTANCE_USER=
DEST_INSTANCE_PASSWORD=

CONFIG_FOLDER=
```

`CONFIG_FOLDER` is intended for caching Dashboards, Questions that you may use on future.

Provide both `SOURCE_INSTANCE_URL` and `DEST_INSTANCE_URL` starting with `https://`
