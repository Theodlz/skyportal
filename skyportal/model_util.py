import sqlalchemy as sa

from baselayer.app.env import load_env
from baselayer.app.psa import TornadoStorage
from skyportal.enum_types import LISTENER_CLASSES, sqla_enum_types
from skyportal.models import ACL, DBSession, Group, Role, Token, User

all_acl_ids = [
    'Become user',
    'Comment',
    'Annotate',
    'Manage users',
    'Manage sources',
    'Manage groups',
    'Manage shifts',
    'Manage instruments',
    'Manage allocations',
    'Manage observing runs',
    'Manage telescopes',
    'Manage Analysis Services',
    'Manage Recurring APIs',
    'Manage observation plans',
    'Manage GCNs',
    'Upload data',
    'Run Analyses',
    'System admin',
    'Post taxonomy',
    'Delete taxonomy',
    'Delete instrument',
    'Delete telescope',
    'Delete bulk photometry',
    'Classify',
] + [c.get_acl_id() for c in LISTENER_CLASSES]


role_acls = {
    'Super admin': all_acl_ids,
    'Group admin': [
        'Annotate',
        'Comment',
        'Manage shifts',
        'Manage sources',
        'Manage Analysis Services',
        'Manage Recurring APIs',
        'Manage GCNs',
        'Upload data',
        'Run Analyses',
        'Post taxonomy',
        'Manage users',
        'Classify',
        'Manage observing runs',
    ],
    'Full user': [
        'Annotate',
        'Comment',
        'Upload data',
        'Classify',
        'Run Analyses',
        'Manage observing runs',
    ],
    'View only': [],
}

env, cfg = load_env()


async def add_user(username, roles=[], auth=False, first_name=None, last_name=None):

    async with DBSession() as session:
        user = await session.scalar(sa.select(User).where(User.username == username))

        if user is None:
            user = User(username=username, first_name=first_name, last_name=last_name)
            if auth:
                TornadoStorage.user.create_social_auth(
                    user, user.username, 'google-oauth2'
                )
            session.add(user)

        for rolename in roles:
            role = await session.scalar(sa.select(Role).where(Role.id == rolename))
            if role not in await session.run_sync(lambda sess: user.roles):
                user.roles.append(role)

        # await session.flush()

        # Add user to sitewide public group
        public_group_name = cfg['misc.public_group_name']
        if public_group_name:
            public_group = await session.scalar(
                sa.select(Group).where(Group.name == public_group_name)
            )
            if public_group is None:
                public_group = Group(name=public_group_name)
                session.add(public_group)
                # await session.flush()
        if public_group not in await session.run_sync(lambda sess: user.groups):
            user.groups.append(public_group)
        await session.commit()

    return await DBSession().scalar(sa.select(User).where(User.username == username))


async def refresh_enums():
    async with DBSession() as session:
        for type in sqla_enum_types:
            for key in type.enums:
                await session.execute(
                    sa.text(f"ALTER TYPE {type.name} ADD VALUE IF NOT EXISTS '{key}'")
                )
        await session.commit()


async def make_super_user(username):
    """Initializes a super user with full permissions."""
    await setup_permissions()  # make sure permissions already exist
    await add_user(username, roles=['Super admin'], auth=True)


async def provision_token():
    """Provision an initial administrative token."""
    admin = await add_user(
        'provisioned_admin',
        roles=['Super admin'],
        first_name="provisioned",
        last_name="admin",
    )
    token_name = 'Initial admin token'

    token = await DBSession().scalar(
        sa.select(Token).where(
            Token.created_by_id == admin.id, Token.name == token_name
        )
    )

    if token is None:
        token_id = await create_token(all_acl_ids, user_id=admin.id, name=token_name)
        token = await DBSession().scalar(sa.select(Token).where(Token.id == token_id))

    return token


async def provision_public_group():
    """If public group name is set in the config file, create it."""
    env, cfg = load_env()
    public_group_name = cfg['misc.public_group_name']
    pg = await DBSession().scalar(
        sa.select(Group).where(Group.name == public_group_name)
    )

    if pg is None:
        DBSession().add(Group(name=public_group_name))
        await DBSession().commit()


async def setup_permissions():
    """Create default ACLs/Roles needed by application.

    If a given ACL or Role already exists, it will be skipped."""
    all_acls = []
    for acl_id in all_acl_ids:
        acl = await DBSession().get(ACL, acl_id)
        if acl is None:
            await DBSession().add(ACL(id=acl_id))
        all_acls.append(acl)
    await DBSession().commit()

    for r, acl_ids in role_acls.items():
        role = await DBSession().get(Role, r)
        if role is None:
            role = Role(id=r)
        acls = []
        for a in acl_ids:
            acl = await DBSession().scalar(sa.select(ACL).where(ACL.id == a))
            acls.append(acl)
        role.acls = acls
        DBSession().add(role)
    await DBSession().commit()


async def create_token(ACLs, user_id, name):
    t = Token(permissions=ACLs, name=name)
    u = DBSession().get(User, user_id)
    u.tokens.append(t)
    t.created_by = u
    DBSession().add(u)
    DBSession().add(t)
    await DBSession().commit()
    return t.id


async def delete_token(token_id):
    t = DBSession().get(Token, token_id)
    if t is not None:
        DBSession().delete(t)
        await DBSession().commit()
