#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from edb import errors

from edb.common.parsing import ListNonterm
from edb.edgeql import ast as qlast

from . import tokens
from .expressions import Nonterm
from .tokens import *  # NOQA
from .expressions import *  # NOQA


class SessionStmt(Nonterm):
    def reduce_SetStmt(self, *kids):
        self.val = kids[0].val

    def reduce_ResetStmt(self, *kids):
        self.val = kids[0].val


class OptSystem(Nonterm):
    def reduce_empty(self, *kids):
        self.val = False

    def reduce_SYSTEM(self, *kids):
        self.val = True


class SetDecl(Nonterm):
    def reduce_ALIAS_Identifier_AS_MODULE_ModuleName(self, *kids):
        self.val = qlast.SessionSetAliasDecl(
            module='.'.join(kids[4].val),
            alias=kids[1].val)

    def reduce_MODULE_ModuleName(self, *kids):
        self.val = qlast.SessionSetAliasDecl(
            module='.'.join(kids[1].val))

    def reduce_OptSystem_CONFIG_Identifier_ASSIGN_Expr(self, *kids):
        self.val = qlast.SessionSetConfigAssignDecl(
            system=kids[0].val,
            alias=kids[2].val,
            expr=kids[4].val)

    def reduce_OptSystem_CONFIG_Identifier_ADDASSIGN_Expr(self, *kids):
        self.val = qlast.SessionSetConfigAddAssignDecl(
            system=kids[0].val,
            alias=kids[2].val,
            expr=kids[4].val)

    def reduce_OptSystem_CONFIG_Identifier_REMASSIGN_Expr(self, *kids):
        self.val = qlast.SessionSetConfigRemAssignDecl(
            system=kids[0].val,
            alias=kids[2].val,
            expr=kids[4].val)


class SetDeclList(ListNonterm, element=SetDecl,
                  separator=tokens.T_COMMA):
    pass


class SetStmt(Nonterm):
    def reduce_SET_SetDeclList(self, *kids):
        has_system_commands = any(
            node.system
            for node in kids[1].val
            if isinstance(node, qlast.BaseSessionConfigSet)
        )

        if has_system_commands and len(kids[1].val) > 1:
            raise errors.EdgeQLSyntaxError(
                "SET supports at most one SET SYSTEM CONFIG command",
                context=kids[0].context)

        self.val = qlast.SetSessionState(
            items=kids[1].val
        )


class ResetDecl(Nonterm):
    def reduce_ALIAS_Identifier(self, *kids):
        self.val = qlast.SessionResetAliasDecl(
            alias=kids[1].val)

    def reduce_MODULE(self, *kids):
        self.val = qlast.SessionResetModule()

    def reduce_ALIAS_STAR(self, *kids):
        self.val = qlast.SessionResetAllAliases()


class ResetDeclList(ListNonterm, element=ResetDecl,
                    separator=tokens.T_COMMA):
    pass


class ResetStmt(Nonterm):
    def reduce_RESET_ResetDeclList(self, *kids):
        self.val = qlast.ResetSessionState(
            items=kids[1].val
        )
