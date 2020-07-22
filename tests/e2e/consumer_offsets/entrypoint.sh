#!/bin/bash
set -e
set -x

produce -maxOffset 2000 $*
consume -initialOffset -2 -markOffset 2001 $*
consume -initialOffset 2001 -markOffset 2002 $*
consume -initialOffset -2 $*
