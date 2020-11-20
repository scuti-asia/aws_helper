require('dotenv').config()

const {MongoClient} = require('mongodb');
const fs = require('fs');
const tunnel = require('tunnel-ssh');
const probe = require('probe-image-size');
const aws = require('aws-sdk');
const execa = require('execa');
const { resolve } = require('path');
const { readdir } = require('fs').promises;
const log = console.log;
let env;

// This function sets up the SSH tunnel and then
// we can call any of the other methods
const main = async () => {
  env = readEnv();

  const config = {
    username: env.linuxUsername,
    host: env.hostStag,
    privateKey: fs.readFileSync(env.awsPemFile),
    port:22,
    dstPort:27017,
    localPort: env.localPort
  };

  let server = tunnel(config, async (error, server) => {

    if (error) {
      log("SSH connection error: " + error);
    }

    await showImageSize();
  });
}

const showImageSize = async () => {
  try {
    const client = new MongoClient(env.mongoUriStag);
    await client.connect();

    const dbResponse = await client.db('nikkei')
        .collection('media')
        .find({
          $and: [
            { $or: [
                {mimetype: "image/png"},
                {mimetype: "image/jpg"},
                {mimetype: "image/jpeg"},
              ]}
            , { $or: [
                {height: ""},
                {width: ""}
              ]}
          ]
        });

    const total = await dbResponse.count();
    let currentMedia = 0;

    while (await dbResponse.hasNext()) {
      const media = await dbResponse.next();

      currentMedia++;
      log(`Processing ${currentMedia}/${total} - ${media.key}`);

      const url = env.bucketStag + media.key;
      const fileName = getFileName(url);
      const extRegex = /(?:\.([^.]+))?$/;
      const fileExtension = extRegex.exec(fileName)[1].toLowerCase();

      if (fileExtension === 'jpg' || fileExtension === 'jpeg' || fileExtension === 'png') {
        try {
          log('Probing url', url)

          let result = await probe(url);

          if (!result.height || !result.width) {
            log(`[x] Result has no dimensions for ${url}.`);
          } else {
            log(`Key: ${fileName} - Dimensions: height (${result.height}) width (${result.width})`);
          }

        } catch (err) {
          log('[x] Caught an error - ', fileName);
        }
      }
    }

  } catch (err) {
    log('[x] Found an error: ', err)
  }
}

const fixImageSize = async () => {
    const client = new MongoClient(env.mongoUriProd);
    await client.connect();

    const dbResponse = await client.db('nikkei')
        .collection('media')
        .find({
          $and: [
            { $or: [
                { mimetype: "image/png" },
                { mimetype: "image/jpg" },
                { mimetype: "image/jpeg" },
              ]} ,
            { $or: [
                { height: "" },
                { width: "" }
              ]}
          ]
        });

    const total = await dbResponse.count();
    let totalErrors = 0;
    let currentMedia = 0;

  while (await dbResponse.hasNext()) {
    const media = await dbResponse.next();
    currentMedia++;
    log(`Processing ${currentMedia}/${total} - ${media.key}`);

    const url = env.bucketProd + media.key;
    const fileName = getFileName(url);
    const extRegex = /(?:\.([^.]+))?$/;
    const fileExtension = extRegex.exec(fileName)[1].toLowerCase();

    try {
      if (fileExtension === 'jpg' || fileExtension === 'jpeg' || fileExtension === 'png') {
        let result = await probe(url);

        if (!result.height || !result.width) {
          log(`[x] Probing returned no dimensions for ${url}.`);
        } else {
          const updateResponse = await client.db('nikkei')
              .collection('media')
              .updateOne({"key": fileName}, {
                $set: {
                  height: `${result.height}`,
                  width: `${result.width}`
                }
              });

          log(`Key: ${fileName} - Found: ${updateResponse.matchedCount} - Modified: ${updateResponse.modifiedCount}`);
        }
      }
    } catch (err) {
      log(`[x] Thrown error for key ${fileName}`);
      totalErrors++;
    }
  }

  if (totalErrors > 0) {
    log(`>>> Process finished with ${totalErrors} errors.`);
  }
}

const uploadFiles = async () => {
    const s3 = new aws.S3({
      accessKeyId: env.accessKeyId,
      secretAccessKey: env.secretAccessKey
    });

    const files = await getFiles('files');
    const total = files.length;
    let currentFile = 0;
    let totalErrors = 0;

    const client = new MongoClient(env.mongoUriStag);
    await client.connect();

    for (let filePath of files) {
      try {
        ++currentFile;
        const fileName = getFileName(filePath);
        log(`Processing file ${currentFile}/${total} (${fileName})`);

        let data;
        const result = await uploadFile(s3, filePath);
        const infoMedia = await getInfoMedia(filePath);
        const size = fs.statSync(filePath).size;
        const extRegex = /(?:\.([^.]+))?$/;
        const fileExtension = extRegex.exec(filePath)[1].toLowerCase();

        if (fileExtension === 'mp4') {
          data = {
            etag: result.ETag,
            duration: `${infoMedia.duration}`,
            size: `${size}`,
          };
        } else if (fileExtension === "jpg" || fileExtension === "png" || fileExtension === "jpeg") {
          data = {
            etag: result.ETag,
            height: `${infoMedia.height}`,
            width: `${infoMedia.width}`,
            size: `${size}`,
          };
        }

        const dbResponse = await client.db('nikkei')
            .collection('media')
            .updateOne({"key": fileName}, {$set: data});

        log(`Found: ${dbResponse.matchedCount} - Modified: ${dbResponse.modifiedCount}`);
      } catch (err) {
        log(`[x] Error uploading file ${filePath}`, err);
        totalErrors++;
      }
    }

    if (totalErrors === 0) {
      log('The process has ended without errors.');
    } else {
      log(`The process ended with ${totalErrors} errors.`);
    }
}

const uploadFile = async (s3, filePath) => {
  // Read content from the file
  const fileContent = fs.readFileSync(filePath);
  const fileName = getFileName(filePath);

  // Setting up S3 upload parameters
  const params = {
    Bucket: env.awsBucketUpload,
    Key: fileName,
    Body: fileContent,
    ACL: 'public-read'
  };

  // Uploading files to the bucket
  const result = await s3.upload(params, (err, data) => {
    if (err) {
      log(`[x] Error uploading file ${filePath}`);
    }
  }).promise();

  return result;
};

const getInfoMedia = async (filePath) => {
  const result = {
    width: 0,
    height: 0,
    duration: 0
  }

  const params = ['-v', 'error', '-show_format', '-show_streams']
  const { stdout } = await execa('ffprobe', [...params, filePath])

  const width = stdout.match(/width="?([0-9]*)"?/)
  if (width && width[1]) {
    result.width = Number(width[1])
  }

  const height = stdout.match(/height="?([0-9]*)"?/)
  if (height && height[1]) {
    result.height = Number(height[1])
  }

  const duration = stdout.match(/duration="?(\d*\.\d*)"?/)
  if (duration && duration[1]) {
    result.duration = duration[1]
  }
  return result
}

const getFiles= async (dir) => {
  const dirents = await readdir(dir, { withFileTypes: true });
  const files = await Promise.all(dirents.map((dirent) => {
    const res = resolve(dir, dirent.name);
    return dirent.isDirectory() ? getFiles(res) : res;
  }));

  return Array.prototype.concat(...files);
}

const getFileName = fullPath => {
  return fullPath.split('\\').pop().split('/').pop();
}

const readEnv = () => {
  return {
    localPort: process.env.LOCAL_PORT,
    mongoUriDev: process.env.MONGO_URI_DEV.replace('local_port', process.env.LOCAL_PORT),
    mongoUriStag: process.env.MONGO_URI_STAG.replace('local_port', process.env.LOCAL_PORT),
    mongoUriProd: process.env.MONGO_URI_PROD.replace('local_port', process.env.LOCAL_PORT),
    bucketDev: process.env.BUCKET_DEV_URL,
    bucketStag: process.env.BUCKET_STAG_URL,
    bucketProd: process.env.BUCKET_PROD_URL,
    hostDev: process.env.HOST_DEV,
    hostStag: process.env.HOST_STAG,
    hostProd: process.env.HOST_PROD,
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    awsBucketUpload: process.env.AWS_BUCKET_UPLOAD,
    awsPemFile: process.env.AWS_PEM_FILE,
    linuxUsername: process.env.LINUX_USERNAME
  }
}

// Entry point of the script
main();
