# Python 3 compatibility
from __future__ import print_function
import sys


def generate_travis_yml(
    language,
    os,
    python_version,
    install_commands,
    script_commands,
    after_success_commands,
):
    """
    生成.travis.yml文件的内容。
    :param language: 使用的语言，通常是"python"
    :param os: 目标操作系统，通常是"linux"
    :param python_version: 使用的Python版本
    :param install_commands: 安装依赖的命令列表
    :param script_commands: 运行测试的命令列表
    :after_success_commands: 成功后运行的命令
    """
    travis_yml_content = """
language: {language}

os:
  - {os}
  
python:
  - {python_version}

install:
  - {install_commands}

script:
  - {script_commands}

after_success:
  - {after_success_commands}

    """.format(
        language=language,
        os=os,
        python_version=python_version,
        install_commands="\n  - ".join(install_commands),
        script_commands="\n  - ".join(script_commands),
        after_success_commands="\n  - ".join(after_success_commands),
    )
    return travis_yml_content


# 示例使用
install_commands = [
    "pip install -r requirements.txt",
    "pip install tox",
    "pip install pytest",
]
script_commands = ["tox"]
after_success_commands = ["echo 'after success'", "coveralls"]
python_version = sys.version.split(" ")[0]
system_type = sys.platform
travis_yml = generate_travis_yml(
    language="python",
    os=system_type,
    python_version=python_version,
    install_commands=install_commands,
    script_commands=script_commands,
    after_success_commands=after_success_commands,
)

# 打印生成的.travis.yml内容
print(travis_yml)

# 将内容写入.travis.yml文件
with open(".travis.yml", "w") as file:
    file.write(travis_yml)
