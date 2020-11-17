# python3
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""spanner_orm setup file."""
from setuptools import setup

setup(
    name="spanner-orm",
    version="0.4.0",
    description="Basic ORM for Spanner",
    maintainer="Aniruddha Maru",
    maintainer_email="aniruddhamaru@gmail.com",
    url="https://github.com/maroux/python-spanner-orm",
    packages=["spanner_orm", "spanner_orm.admin"],
    include_package_data=True,
    python_requires="~=3.7",
    install_requires=["wheel", "google-cloud-spanner >= 1.6, <2.0.0dev"],
    tests_require=["absl-py"],
    extras_require={
        "tests": ["black==20.8b1"],
    },
    entry_points={"console_scripts": ["spanner-orm = spanner_orm.admin.scripts:main"]},
)
