with open('test.csv','r') as f:
    size = 0
    oldQID = "";
    userIds = []
    for line in f:
        size += 1
        tup = line.split(',')
        QID = tup[1]+tup[2]+tup[3]+tup[4]+tup[5]+tup[6]
        if (QID == oldQID):
            if (tup[0] in userIds):
                print("User id {} occured twice in QID {}".format(tup[0], QID))
            else:
                userIds.append(tup[0])
        else:
            oldQID = QID
            userIds = []








print("Tuples: {}".format(size))

