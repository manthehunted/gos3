import sys
import asyncio
import aioboto3


async def main(bucket_str, prefix):
    session = aioboto3.Session()
    async with session.resource("s3") as s3:
        bucket = await s3.Bucket(bucket_str)

        async for s3_object in bucket.objects.filter(Prefix=prefix):
            fs = s3_object.key.split("/")[-1]
            await s3.meta.client.download_file(bucket_str, s3_object.key, fs)


if __name__ == "__main__":
    bucket, prefix = sys.argv[1:]
    asyncio.run(main(bucket, prefix))
