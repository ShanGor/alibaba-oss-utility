# Alibaba Cloud OSS utility

项目缘起：由于我们要用阿里的大数据平台，MaxCompute + Dataworks。而Dataworks处理不了我们有些需求，于是需要自己写代码来跑。那写好之后放哪里呢？用什么跑呢？
经研究，Dataworks的Shell功能比较适合，但是Shell不支持ossuti这个官方工具，即使下载到里面也不好使。试了Python发现也有版本问题。于是用Java写个小工具，这个工具可以给所有人用。然后项目的程序就放在OSS里面不公开。
> ## Dataworks:  
>  - #### Shell:  
>    - 用`curl -O`下载此工具，此工具为公开读。
>    - 用此工具下载你的应用，而你的应用放在OSS，只有你自己有权限读
>    - 跑你的私密应用来进行OSS和MaxCompute等的各种骚操作
>
>  - #### ODPS ETL
>  - #### 大功告成

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