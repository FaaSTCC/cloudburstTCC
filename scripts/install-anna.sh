#!/bin/bash

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

cd
git config --global user.name "taraslykhenko"
git config --global user.password "b718ab81fae7cf4476e12ec6b9dd7c23ba3e3517"
git clone --recurse-submodules https://github.com/TarasLykhenko/AnnaTCC.git
cd AnnaTCC/client/python
python3 setup.py install
cd ~
rm -rf AnnaTCC
