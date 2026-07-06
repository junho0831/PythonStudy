import pandas as pd

class DatabaseManager:
    # 예시용 더미 클래스입니다. 실제 사용하시는 환경에 맞게 수정해주세요.
    def select(self, query):
        pass

class QueryExecutor:
    def __init__(self):
        self.__db_mgr = DatabaseManager()

    def execute_query(self):
        query = """
            SELECT
                eq_name AS eqp_id,
                title,
                value
            FROM
                genome.genome_n_currentmcfc
            WHERE
                file_type = 'ruip_ipez'
                AND title IN (
                    'mcruip_ipez_exclusionzones_borderwidth_tag',
                    'mcruip_ipez_exclusionzones_eipcornerwidth_tag',
                    'mcruip_ipez_exclusionzones_eipcornerlength_tag'
                )
        """

        # 1. DB에서 원본 DataFrame 그대로 가져오기
        genome_df = self.__db_mgr.select(query)
        
        if genome_df is None or genome_df.empty:
            return pd.DataFrame()
        
        # 2. 순수 판다스 기능(pivot)으로 한 방에 설비당 1줄로 묶기 (딕셔너리 과정 생략!)
        pivoted_df = genome_df.pivot(index='eqp_id', columns='title', values='value')
        
        # 3. 컬럼 이름을 우리가 쓰기 편한 이름으로 변경
        pivoted_df = pivoted_df.rename(columns={
            'mcruip_ipez_exclusionzones_eipcornerlength_tag': 'corner_length',
            'mcruip_ipez_exclusionzones_eipcornerwidth_tag': 'corner_width',
            'mcruip_ipez_exclusionzones_borderwidth_tag': 'border_width'
        })
        
        # 피벗된 판다스 DataFrame을 그대로 리턴
        return pivoted_df

if __name__ == "__main__":
    executor = QueryExecutor()
    df = executor.execute_query()
    print("Query executed.")
