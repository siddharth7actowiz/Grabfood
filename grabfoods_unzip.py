import zipfile
with zipfile.ZipFile("grab_food_pages.zip","r")as z:
    z.extractall()

