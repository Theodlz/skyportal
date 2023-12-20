# DEMO HANDLER FOR ASYNC SQLALCHEMY
from baselayer.app.handlers import BaseHandler
import time
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy as sa
from skyportal.models import Obj


async def main_loop(iterations, session: AsyncSession):
    start = time.time()
    await session.scalars(sa.select(Obj.id))
    for i in range(iterations):
        objs = await session.scalars(sa.select(Obj.id))
        objs = [id for id in objs.all()]
    end = time.time()
    total = end - start
    return f"took {total} seconds to query the Obj table {iterations} times"


class AsyncHandler(BaseHandler):
    async def get(self, inpath=None):
        if inpath is None:
            inpath = 1
        iterations = int(inpath)
        async with self.Session() as session:
            result = await main_loop(iterations, session=session)
        self.write(result)
        self.finish()
