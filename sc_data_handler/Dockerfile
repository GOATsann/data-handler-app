# FROM public.ecr.aws/lambda/python:3.10

# # Install dependencies for TA-Lib
# RUN yum -y update && \
#     yum install -y \
#     gcc \
#     make \
#     wget \
#     tar \
#     gzip

# # Download and install TA-Lib C library
# RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
#     tar -xzf ta-lib-0.4.0-src.tar.gz && \
#     cd ta-lib && \
#     ./configure --prefix=/usr && \
#     make && \
#     make install

# # Install the TA-Lib Python package
# RUN pip install --no-cache-dir TA-Lib

# # Commands to build the docker and upload image
# # docker build . -t goatsann/talib_image:latest --platform linux/amd64
# # docker image push goatsann/talib_image:latest

# TA-LIB 0.4.0 was built into this docker image with public.ecr.aws/lambda/python:3.10 as the base image with the Dockerfile commands above.
FROM goatsann/talib_image:latest
COPY . ./
RUN python3.10 -m pip install -r requirements.txt -t .

# Command can be overwritten by providing a different command in the template directly.
CMD ["app.lambda_handler"]
