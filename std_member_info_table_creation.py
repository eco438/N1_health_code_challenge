from config import  STATES, SHOULDCREATETABLE
import sqlite3
import re

class StdMemberInfoTableCreate:
    
    def __init__(self,database):
        self.id = None
        self.conn =database
        self.cursor =self.conn.cursor()

    @classmethod
    def __preprocessData(cls,batchList):
        processedRow = []
        for row in batchList:
            rowData = list(row)
            if rowData[5] in STATES:
                rowData[5] = STATES[rowData[5]]
            date = rowData[2]
            r = re.compile('\d{1,2}/\d{2}/\d{4}')
            dashString = "-"

            if r.match(date):
               oldDate = date.split("/")
               newDate = oldDate[2]+dashString+oldDate[0]+dashString+oldDate[1]
               rowData[2] = newDate
            processedRow.append(tuple(rowData))
        return processedRow
    
    def createTable(self):
        sql = """
            CREATE TABLE IF NOT EXISTS std_member_info (
                member_id INTEGER PRIMARY KEY,
                member_first_name TEXT,
                member_last_name TEXT,
                date_of_birth DATE,
                main_address TEXT,
                city TEXT,
                state TEXT,
                zip_code INTEGER,
                payer TEXT
            )
        """
        self.cursor.execute(sql)
    
    def insertData(self,table,tableCursor):
        sqlQuery = "SELECT first_name, last_name, dob, street_address,city,state,zip,payer from {tableName} where ( DATE(eligibility_end_date)  >= '2022-04-01' and  DATE(eligibility_start_date) < '2022-05-01') or ( DATE(substr(eligibility_end_date, 7, 4) || '-' || substr(eligibility_end_date, 4, 2) || '-' || substr(eligibility_end_date, 1, 2))  >= '2022-04-01' and DATE(substr(eligibility_start_date, 7, 4) || '-' || substr(eligibility_start_date, 4, 2) || '-' || substr(eligibility_start_date, 1, 2))  <  '2022-05-01')".format(tableName=table)
        
        tableCursor.execute(sqlQuery)
        while True:
            rowData = tableCursor.fetchmany(300)
            if not rowData:
                print("{tableName} is done.".format(tableName=table))
                break
            preprocessedRowData = self.__preprocessData(rowData)
            self.__insertAll(preprocessedRowData)


    def deleteTable(self):
        sql = """
            DROP TABLE IF EXISTS std_member_info
        """
        self.cursor.execute(sql)
   
    def __insertAll(self,batchList):
        sql = """
            INSERT INTO std_member_info (member_first_name,member_last_name, date_of_birth,main_address,city,state,zip_code,payer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.cursor.executemany(sql,batchList)
        self.conn.commit()

    
def createTableAndInsertData(stdMemberInfoTableCreate):
    stdMemberInfoTableCreate.createTable()
    with sqlite3.connect("interview.db") as connection:
        cur = connection.cursor()
        table_list = [table for table in cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
        roster_table = [table_list[i][0] for i in range(1,len(table_list))]
        for roster in roster_table:
            stdMemberInfoTableCreate.insertData(roster,cur)
        cur.close
    print("\n")



def getDistinctMemberCount(cursor):
    return cursor.execute("SELECT COUNT(member_first_name) from (select DISTINCT member_first_name, member_last_name, date_of_birth,main_address,city,state,zip_code, payer from std_member_info)").fetchone()[0]

def getDuplicateMemberCount(cursor):
    return cursor.execute("SELECT COUNT(member_id) FROM (SELECT * FROM std_member_info GROUP BY member_first_name,member_last_name, date_of_birth,main_address,city,state,zip_code HAVING COUNT(member_id) > 1)").fetchone()[0]

def getMemberBreakdownByPayer(cursor):
    return cursor.execute("SELECT payer, COUNT(member_first_name) FROM (select DISTINCT member_first_name, member_last_name, date_of_birth,main_address,city,state,zip_code, payer from std_member_info) GROUP BY payer").fetchall()

def getMembersWithAFoodAccessScoreLowerThanTwoCount(cursor):
    return cursor.execute("SELECT COUNT(member_first_name) from (SELECT  DISTINCT member_first_name, member_last_name, date_of_birth,main_address,city,state,zip_code, payer, food_access_score from std_member_info left join model_scores_by_zip on zip_code = zcta Where food_access_score < 2 ) ").fetchone()[0]

def getAverageSocialIsolationScore(cursor):
    return cursor.execute("SELECT AVG(social_isolation_score) from (SELECT  *, social_isolation_score as social_isolation_score from std_member_info left join model_scores_by_zip on zip_code =zcta group by member_first_name,member_last_name, date_of_birth,main_address,city,state,zip_code, payer )  ").fetchone()[0]

def getMembersWithMaxAlogrexSdohCompositeScore(cursor):
    return cursor.execute(" SELECT std_member_info.*, algorex_sdoh_composite_score as composite_score from std_member_info left join interview.model_scores_by_zip on zip_code = zcta where algorex_sdoh_composite_score = (SELECT MAX( algorex_sdoh_composite_score) FROM model_scores_by_zip) group by member_first_name,member_last_name, date_of_birth,main_address,city,state,zip_code, payer").fetchall()       


def getStdMemberInfoTableAnalysis(cursor):
    print("1. The number of distinct members that are eligible in April 2022 is {}.".format(getDistinctMemberCount(cursor)))
    print("2. The number of members that were added in more than one is {}.".format(getDuplicateMemberCount(cursor)))
    print("3. The breakdown of members by payer: ")
    for row in getMemberBreakdownByPayer(cursor):
        print("\t\tPayer {}: {}.".format(row[0],row[1]))
    print("4. The number of members thst live in a zip code with a food_access_score lower than 2 is {}.".format(getMembersWithAFoodAccessScoreLowerThanTwoCount(cursor)))
    print("5. The average social isolation score is {}.".format(getAverageSocialIsolationScore(cursor)))
    membersWithMaxAlgorexScoreRowData = getMembersWithMaxAlogrexSdohCompositeScore(cursor)
    print("6. The highest algorex_sdoh_composite_score is {} in zip code {}. The members with the highest algorex_sdoh_composite_score are: ".format(membersWithMaxAlgorexScoreRowData[0][-1],membersWithMaxAlgorexScoreRowData[0][-3]))
    for row in membersWithMaxAlgorexScoreRowData:
        memberInformation = "\t\tMemberId: {}, FirstName: {}, LastName: {}, DOB: {}, MainAddress: {}, City: {}, State: {}".format(*row)
        print(memberInformation)
    
if __name__ == '__main__':
    with sqlite3.connect('interview_answer.db',detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES) as conn:
        if SHOULDCREATETABLE:
            stdMemberInfoTableCreate = StdMemberInfoTableCreate(conn)
            stdMemberInfoTableCreate.deleteTable()
            print("Creating std member info table and inserting data into the table.")
            createTableAndInsertData(stdMemberInfoTableCreate)
        
        print("Analysis of std member info table.")
        conn.execute('ATTACH DATABASE ? AS interview',("interview.db",))
        cursor = conn.cursor()
        getStdMemberInfoTableAnalysis(cursor)
        cursor.close()
        
    
