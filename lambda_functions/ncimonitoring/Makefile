NAME = "verypossible/serverless:1.17"

ENVDIR=envs
LIBS_DIR=serverless/lib
PROJECT=supersecret

.PHONY: clean \
    deploy \
    env-dir \
    shell \
    test \
    test-watch \
    libs

run = docker run --rm -it \
        -v `pwd`:/code \
        --env ENV=$(ENV) \
        --env-file envs/$2 \
        --name=$(PROJECT)-serverless-$(ENV) $(NAME) $1

shell : check-env env-dir
    $(call run,bash,$(ENV))

env-dir :
    @test -d $(ENVDIR) || mkdir -p $(ENVDIR)

clean :
    @test -d $(LIBS_DIR) || mkdir -p $(LIBS_DIR)
    rm -rf $(LIBS_DIR)/*

# make libs should be run from inside the container
libs :
    @test -d $(LIBS_DIR) || mkdir -p $(LIBS_DIR)
    pip install -t $(LIBS_DIR) -r requirements.txt
    rm -rf $(LIBS_DIR)/*.dist-info
    find $(LIBS_DIR) -name '*.pyc' | xargs rm
    find $(LIBS_DIR) -name tests | xargs rm -rf

# NOTE:
#
#   Deployments assume you are already running inside the docker container
#
deploy : check-env
    cd serverless && sls deploy -s $(ENV)

# Note the ifndef must be unindented
check-env:
ifndef ENV
    $(error ENV is undefined)
endif
