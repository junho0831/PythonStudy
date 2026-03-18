import getpass

import psycopg2
from sqlalchemy import create_engine, text

DB = "pythonstudy_demo"
USER = getpass.getuser()
ADMIN_DSN = f"postgresql://{USER}@127.0.0.1:5432/postgres"
SA_URL = f"postgresql+psycopg2://{USER}@127.0.0.1:5432/{DB}"
PG_DSN = f"postgresql://{USER}@127.0.0.1:5432/{DB}"
QUERY = "select * from eeseuv_peesphd.v_photo_eqp_info_euv order by eqp_id"


def bootstrap() -> None:
    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("select 1 from pg_database where datname = %s", [DB])
            if not cur.fetchone():
                cur.execute(f'create database "{DB}"')
    finally:
        conn.close()
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("create schema if not exists eeseuv_peesphd")
        cur.execute("create table if not exists eeseuv_peesphd.photo_eqp_info_euv (eqp_id text primary key, eqp_name text not null, exposure_count integer not null)")
        cur.execute(
            """
            insert into eeseuv_peesphd.photo_eqp_info_euv (eqp_id, eqp_name, exposure_count)
            values ('EUV-01', 'Demo Tool', 12), ('EUV-02', 'Demo Tool 2', 34)
            on conflict (eqp_id) do update
            set eqp_name = excluded.eqp_name, exposure_count = excluded.exposure_count
            """
        )
        cur.execute(
            """
            create or replace view eeseuv_peesphd.v_photo_eqp_info_euv as
            select eqp_id, eqp_name, exposure_count from eeseuv_peesphd.photo_eqp_info_euv
            """
        )


def run_psycopg2() -> None:
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(QUERY)
        print("[psycopg2 direct] ok=True")
        print(cur.fetchall())


def run_sqlalchemy_direct() -> None:
    with create_engine(SA_URL).connect() as conn:
        print("[sqlalchemy direct] ok=True")
        print(conn.execute(text(QUERY)).fetchall())


def run_broken_sqlalchemy_wrapper() -> None:
    try:
        stmt = text(QUERY)
        if stmt:
            pass
    except Exception as exc:
        print("[sqlalchemy wrapper boolean] ok=False")
        print(f"{type(exc).__name__}: {exc}")
    try:
        with create_engine(SA_URL).connect() as conn:
            conn.execute(text("set search_path to public"))
            conn.execute(text("select * from eeseuv_peesphd.v_photo_eqp_info_euv")).fetchall()
    except Exception as exc:
        print("[sqlalchemy wrapper relation] ok=False")
        print(f"{type(exc).__name__}: {exc}")


def main() -> None:
    bootstrap()
    print(f"database={DB}")
    print(f"query={QUERY}")
    run_psycopg2()
    run_sqlalchemy_direct()
    run_broken_sqlalchemy_wrapper()
    print("문제의 핵심은 SQL 문 자체보다 SQLAlchemy execute 경로 또는 그 wrapper가 psycopg2 직접 실행과 다르게 동작한다는 점입니다.")


if __name__ == "__main__":
    main()
