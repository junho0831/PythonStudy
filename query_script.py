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

        # select()가 DataFrame을 반환한다고 가정
        genome_df = self.__db_mgr.select(query)
        
        # 결과를 담을 딕셔너리
        # 구조: { 'eqp_id': {'corner_length': val, 'corner_width': val, 'border_width': val} }
        result_data = {}
        
        if genome_df is not None and not genome_df.empty:
            for _, row in genome_df.iterrows():
                eqp_id = row['eqp_id']
                title = row['title']
                value = row['value']
                
                # eqp_id가 처음 등장하면 딕셔너리에 초기화
                if eqp_id not in result_data:
                    result_data[eqp_id] = {
                        'corner_length': None,
                        'corner_width': None,
                        'border_width': None
                    }
                
                # 쿼리의 title 값에 따라 알맞은 키에 값 할당
                if title == 'mcruip_ipez_exclusionzones_eipcornerlength_tag':
                    result_data[eqp_id]['corner_length'] = value
                elif title == 'mcruip_ipez_exclusionzones_eipcornerwidth_tag':
                    result_data[eqp_id]['corner_width'] = value
                elif title == 'mcruip_ipez_exclusionzones_borderwidth_tag':
                    result_data[eqp_id]['border_width'] = value
                    
        return result_data

if __name__ == "__main__":
    executor = QueryExecutor()
    df = executor.execute_query()
    print("Query executed.")
