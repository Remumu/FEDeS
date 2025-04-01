#!/bin/bash

RELEASE=2
MAIN_VERS=22
SUB_VERS=2


WD="$(pwd)"

#EXECUTE_NAME="1g5n-v${RELEASE}.${MAIN_VERS}.${SUB_VERS}-ap25"
# EXECUTE_NAME="sim-v${RELEASE}.${MAIN_VERS}.${SUB_VERS}-debug"
EXECUTE_NAME="simulate"

echo "compile ${EXECUTE_NAME}"

cd "${WD}"/simulator
make
cp main ../${EXECUTE_NAME}
make clean

