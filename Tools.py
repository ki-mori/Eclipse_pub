# -*- coding:utf-8 -*-

"""
-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

        様々なツール

        別途インストールが必要なモジュール
            ・psycopg2
            ・xlrd
            ・pandas
            ・PIL
            ・PypDF2
    UpdateDate:2020/02/27
    実行環境：Eclipse,Python3.8

-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
"""
import os
from pathlib import Path
from pathlib import PurePath
import shutil
import psycopg2
import sqlite3
import xlrd
import csv
import datetime
import re
import sys
import subprocess
import pandas
import PyPDF2
from PIL import Image
from PIL.ExifTags import TAGS

# ---------------------------------------------------------------------------------
"""ファイル検索関連"""
class File_Search_Function():
# ---------------------------------------------------------------------------------
    """ファイル検索（フォルダパス,拡張子） リターン：フルパス"""
    def File_Search_Ext(self,file_path,file_ext):
        full_path_name =[]
        p = Path(file_path)
        pdf_list = list(p.glob('**/*' + file_ext))
        for file in pdf_list:
            fname = PurePath(file).name
            root = PurePath(file).parent
            full_path_name.append(str(Path(root,Path(str(PurePath(file).name)))))
        return full_path_name
# ---------------------------------------------------------------------------------
"""エクスプローラー関連"""
class Explorer_Job():
# ---------------------------------------------------------------------------------
    """ファイルコピーと貼り付け（コピー元ファイル名,ペースト先ファイル名）"""
    def File_Copy(self,copy_path,paste_path):
        shutil.copyfile(copy_path, paste_path)
# ---------------------------------------------------------------------------------
    """コマンドプロンプトからGDAL TRANSLATEを実行（インプットフォルダパス,インプットファイルの拡張子,アウトプットフォルダパス）"""
    def Gdal_Translate(self, IN_Path, IN_EXT, OUT_Path):
        PathName = []
        FileName = []
        BinPath = 'C:/OSGeo4W/bin'
        BinPath64 = 'C:/OSGeo4W64/bin'
        if os.path.isdir(BinPath):
            pass
        else:
            if os.path.isdir(BinPath64):
                BinPath = BinPath64
            else:

                sys.exit(1)
        f1 = File_Search_Function()
        c1 = f1.File_Search_Ext(IN_Path, IN_EXT)
        cnt = 0
        for file in c1:
            head,fname = os.path.split(file)
            PathName.append(head)
            FileName.append(fname)
            cnt = cnt + 1
            maxlen = len(FileName)
        fcnt = 1
        while fcnt < maxlen:
            ImgFileName = Path(PathName[fcnt],FileName[fcnt])
            OutFileName = Path(OUT_Path,FileName[fcnt])
            cmd1 = 'cd %s' %(BinPath)
            subprocess.call(cmd1, shell = True)
            cmd2 ='gdal_translate -co COMPRESS=DEFLATE -co PREDICTOR=2 -co ZLEVEL=9 -co tfw=yes -of GTiff %s %s' % (ImgFileName, OutFileName)
            subprocess.call(cmd2, shell = True)
            fcnt = fcnt + 1
# ---------------------------------------------------------------------------------
"""データベース関連"""
class Database_Connection():
# ---------------------------------------------------------------------------------
    """Postgresql接続（IPアドレス,ポート,データベース名,ユーザー名,パスワード） リターン：接続"""
    def Pgsql_Con(self, db_host, db_port ,db_name, db_user, db_pass):
        dsn = 'host=' + db_host + ' port=' + db_port + ' dbname=' + db_name + ' user=' + db_user + ' password=' + db_pass
        return psycopg2.connect(dsn)
# ---------------------------------------------------------------------------------
    """SQLITE接続（データベースパス,データベース名（拡張子付）） リターン：接続"""
    def Sqlite_Con(self,db_path, db_name):
        dsn = Path(db_path, db_name)
        return sqlite3.connect(dsn)
# ---------------------------------------------------------------------------------
"""EXCELファイル関連"""
class Excel_File_Function():
# ---------------------------------------------------------------------------------
    """EXCELファイルの全シートをCSVファイルへ変換
        （EXCELファイルのフルパス,CSVアウトプットパス（フォルダ作成））"""
    def xlsx2csv(self, IN_FilePath, OUT_Path):
        c1 = Excel_File_Function()
        wb = xlrd.open_workbook(IN_FilePath)
        os.makedirs(OUT_Path, exist_ok=True)
        for sheet in wb.sheets():
            sheet_name = sheet.name
            dest_path = Path(OUT_Path, sheet_name + '.csv')
            print(dest_path)
            with open(dest_path, 'w', encoding='utf-8') as fp:
                writer = csv.writer(fp)
                for row in range(sheet.nrows):
                    li = []
                    for col in range(sheet.ncols):
                        cell = sheet.cell(row, col)
                        if cell.ctype == xlrd.XL_CELL_NUMBER:
                            val = cell.value
                            if val.is_integer():
                                val = int(val)
                        elif cell.ctype == xlrd.XL_CELL_DATE:
                            dt = c1.Get_DT_From_Serial(cell.value)
                            val = dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            val = cell.value
                            li.append(val)
                    writer.writerow(li)
# ---------------------------------------------------------------------------------
    """シリアル値を時刻へ変換（シリアル値） リターン：時刻"""
    def Get_DT_From_Serial(self, serial):
        base_date = datetime.datetime(1899, 12, 30)
        d, t = re.search(r'(\d+)(\.\d+)', str(serial)).groups()
        return base_date + datetime.timedelta(days=int(d)) + datetime.timedelta(seconds=float(t) * 86400)
# ---------------------------------------------------------------------------------
    """エクセルファイルの全シートを１ファイルにシート名で分割
    （分割したいEXCELのフォルダーパス,分割したいEXCELファイル名,出力先（フォルダ指定）,出力拡張子）"""
    def ExcelFile_Split_XLSX(self, Input_Path, Input_FileName, Output_Folder, Out_EXT):
        XL_PATH = Input_Path
        Filepath = Path(XL_PATH, Input_FileName)
        Out_Path = Output_Folder
        wb = pandas.ExcelFile(Filepath)
        os.makedirs(Out_Path, exist_ok=True)

        for sheet_name in wb.sheet_names:
            print(sheet_name)
            df = wb.parse(sheetname=sheet_name)
            Output_Path = Path(Out_Path, sheet_name + '.' + Out_EXT)
            print(Output_Path )
            df.to_excel( Output_Path )
# ---------------------------------------------------------------------------------
"""その他"""
class Database_Exchange():
    """辞書テーブルと一致する文字列置換（大文字⇒小文字）
    （元のテキスト,置換後テキスト） リターン：置換後テキスト

        辞書テーブル使用例
        変数名 = {置換前 : 置換後}
    trans_code = {'地番'   :  'tiban',
                  '町丁CD' :  'c_code'}
    """
    def Text_Replace(self, text, dic):
        for i, j in dic.items():
            print(i, j)
            text = text.replace(i, j)
            text = text.lower()
        return text
    """大文字に変換（テキスト） リターン：置換後テキスト"""
    def Text_Up(self, text):
        text = text.upper()
        return text
    """小文字に変換（テキスト） リターン：置換後テキスト"""
    def Text_Low(self, text):
        text = text.lower()
        return text

class System_Support():
    def xrange(self, *args, **kwargs):
        if sys.version_info >= (3, 0):
            return iter(range(*args, **kwargs))
# ---------------------------------------------------------------------------------
"""PDFファイル関連"""
class PDF_Job():
    def __init__(self,gid,full_path,siryo_num,file_name,pages,chiku,file_link):
        self.gid = gid
        self.full_path = full_path
        self.siryo_num = siryo_num
        self.file_name = file_name
        self.pages = pages
        self.chiku = chiku
        self.file_link = file_link
    """表示オプション変更（PDFファイルのフルパス）"""
    """"PDFファイルのページレイアウト、ページモードを変更
        ※事前にPyPDF2のpdf.pyを修正する必要がある。
    """
    def PDF_LayoutChange(self, pdf_file):
        pdf_file_obj = open(pdf_file, 'rb')
        f_reader = PyPDF2.PdfFileReader(pdf_file_obj)
        f_writer = PyPDF2.PdfFileWriter()
        pg = f_reader.getNumPages()
        rt = f_reader.getPageLayout()
        gt = f_reader.getPageMode()
        print(pdf_file, rt, gt, pg)
        f_writer.cloneDocumentFromReader(f_reader)
        f_writer.setPageLayout('/SinglePage')
        f_writer.setPageMode('/UseThumbs')
        f_writer.write(open(pdf_file,'wb'))
        pg = f_reader.getNumPages()
        rt = f_reader.getPageLayout()
        gt = f_reader.getPageMode()
        print(pdf_file, rt, gt, pg)

#=======================================================================================================================
#     def PDF_Information(self, pdf_path):
#         with open(pdf_path, 'rb') as f:
#             f_reader = PyPDF2.PdfFileReader(f)
#             information = f_reader.getDocumentinfo()
#             number_of_pages = r_reader = f_reader.getNumPages()
#         txt = f"""
#         Information about {pdf_path}:
#
#         Author: {information.author}
#         Creator: {information.creator}
#         Producer: {information.producer}
#         Subject: {information.subject}
#         Title: {information.title}
#         Number of pages: {number_of_pages}
#         """
#
#         print(txt)
#         return information
#=======================================================================================================================
# ---------------------------------------------------------------------------------
"""JPGファイル関連"""
class JPG_Job():
    def Get_Exif(self):
        ret = {}
        i = Image.open(self)
        info = i._getexif()
        for tag, value in info.items():
            decoded = TAGS.get(tag, tag)
            ret[decoded] = value
        return ret