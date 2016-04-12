#!/bin/bash

javac TestInput.java
#java TestInput /home/hduser/input_file /user/gurunath/temp
java TestInput $1 $2
chmod +x output.sh
./output.sh
rm output.sh
