
all:	codec_pb2.py

clean:
	rm -f codec_pb2.py*

codec_pb2.py:	codec.proto
	protoc --python_out=. codec.proto

