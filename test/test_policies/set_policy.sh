#!/bin/bash

if [ $# -ne 5 ]; then
    echo "usage: ./set_policy <bucket_name> <userid> <ownerid> <prefix> <template>"
    echo "Example: ./set_policy my-bucket 123456 789012 my-folder policy.json"
    exit 1
fi

BUCKET_NAME=$1
USER_ID=$2
OWNER_ID=$3
PREFIX=$4
POLICY_FILE=$5

if [ ! -f "$POLICY_FILE" ]; then
    echo "Error: cannot find $POLICY_FILE"
    exit 1
fi

cp "$POLICY_FILE" "${POLICY_FILE}.backup"

sed -i "s/@BUCKETNAME/${BUCKET_NAME}/g" "$POLICY_FILE"
sed -i "s/@USERID/${USER_ID}/g" "$POLICY_FILE"
sed -i "s/@OWNERID/${OWNER_ID}/g" "$POLICY_FILE"
sed -i "s/@PREFIX/${PREFIX}/g" "$POLICY_FILE"

echo "new policy is saved in $POLICY_FILE"
echo "The original template is backed up in ${POLICY_FILE}.backup"
