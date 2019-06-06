set -e
DEST=gs://constellation-dag/release/dag-$1.jar
gsutil cp target/scala-*/constellation-assembly-*.jar $DEST
gsutil acl ch -u AllUsers:R gs://constellation-dag/release/dag-$1.jar
echo "Uploaded to $DEST"