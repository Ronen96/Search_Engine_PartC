import os


class ConfigClass:
    def __init__(self):
        # self.corpusPath = 'C:\\Users\\barif\\PycharmProjects\\Search_Engine\\Data\\date=07-08-2020'
        self.corpusPath = 'C:\\Users\\barif\\PycharmProjects\\Search_Engine_PartC\\data'
        self.savedFileMainFolder = 'C:\\Users\\barif\\PycharmProjects\\Search_Engine_PartC'
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        if not os.path.exists(self.saveFilesWithStem):
            os.mkdir(self.saveFilesWithStem)
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        if not os.path.exists(self.saveFilesWithoutStem):
            os.mkdir(self.saveFilesWithoutStem)

        self.toStem = False

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath
