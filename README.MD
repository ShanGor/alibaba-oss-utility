# Alibaba Cloud OSS utility

Before run, please prepare environment variables
- Prepare the `$HOME/.ossutilconfig`  
  At least have below (value is sample):  
  - `endpoint=oss-cn-shenzhen.aliyuncs.com`
  - `accessKeyID=LTAI4GHyxjjCGZQMKTh82d7x`
  - `accessKeySecret=***`

## Usage:
java -jar oss-utility.jar parameters  
> Parameters
>- ls oss://bucket-name/path
>- download oss://bucket-name/path local-path
>- upload local-path oss://bucket-name/path