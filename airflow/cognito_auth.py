# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import flask_login

# Need to expose these downstream
# flake8: noqa: F401
from flask_login import current_user, logout_user, login_required, login_user

from flask import url_for, redirect, request

import cognitojwt

from airflow import models
from airflow.configuration import conf
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log


def get_config_param(param):
    return str(conf.get('cognito', param))


class CognitoUser(models.User):

    def __init__(self, user):
        self.user = user

    @property
    def is_active(self):
        """Required by flask_login"""
        return True

    @property
    def is_authenticated(self):
        """Required by flask_login"""
        return True

    @property
    def is_anonymous(self):
        """Required by flask_login"""
        return False

    def get_id(self):
        """Returns the current user id as required by flask_login"""
        return self.user.get_id()

    def data_profiling(self):
        """Provides access to data profiling tools"""
        return True

    def is_superuser(self):
        """Access all the things"""
        return True


class AuthenticationError(Exception):
    pass


class CognitoAuthBackend(object):

    def __init__(self):
        # self.google_host = get_config_param('host')
        self.login_manager = flask_login.LoginManager()
        self.login_manager.login_view = 'airflow.login'
        self.flask_app = None
        self.cognito_oauth = None
        self.api_rev = None

    def init_app(self, flask_app):
        self.flask_app = flask_app

        self.login_manager.init_app(self.flask_app)

        self.region = get_config_param('region')
        self.user_pool_id = get_config_param('user_pool_id')
        self.app_client_id = get_config_param('app_client_id')

        self.login_manager.user_loader(self.load_user)

        # TODO: Implement using a library
        self.flask_app.add_url_rule(get_config_param('oauth_callback_route'),
                                    'cognito_oauth_callback',
                                    self.oauth_callback)

    def login(self, request):
        log.debug('Redirecting user to Cognito login')
        # TODO: Implament using a library
        return self.google_oauth.authorize(callback=url_for(
            'cognito_oauth_callback',
            _external=True),
            state=request.args.get('next') or request.referrer or None)

    def get_cognito_user_profile_info(self, jwt_token):
        resp = cognitojwt.decode(
            jwt_token,
            self.region,
            self.user_pool_id,
            app_client_id=self.app_client_id,  # Optional
            testmode=True  # Disable token expiration check for testing purposes
        )

        if not resp or resp.status != 200:
            raise AuthenticationError(
                'Failed to fetch user profile, status ({0})'.format(
                    resp.status if resp else 'None'))

        return resp.data['name'], resp.data['email']

    def domain_check(self, email):
        domain = email.split('@')[1]
        domains = get_config_param('domain').split(',')
        if domain in domains:
            return True
        return False

    @provide_session
    def load_user(self, userid, session=None):
        if not userid or userid == 'None':
            return None

        user = session.query(models.User).filter(
            models.User.id == int(userid)).first()
        return CognitoUser(user)

    @provide_session
    def oauth_callback(self, session=None):
        log.debug('Cognito OAuth callback called')

        next_url = request.args.get('state') or url_for('admin.index')

        # TODO: Implement using a library
        resp = self.cognito_oauth.authorized_response()

        try:
            if resp is None:
                raise AuthenticationError(
                    'Null response from Cognito, denying access.'
                )

            jwt_token = resp['access_token']

            username, email = self.get_cognito_user_profile_info(jwt_token)

            if not self.domain_check(email):
                return redirect(url_for('airflow.noaccess'))

        except AuthenticationError:
            return redirect(url_for('airflow.noaccess'))

        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                email=email,
                is_superuser=False)

        session.merge(user)
        session.commit()
        login_user(CognitoUser(user))
        session.commit()

        return redirect(next_url)


LOGIN_MANAGER = CognitoAuthBackend()


def login(self, request):
    return LOGIN_MANAGER.login(request)