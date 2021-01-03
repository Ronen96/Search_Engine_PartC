class ConfigClass:
    def __init__(self):
        self.corpusPath = 'C:\\Users\\barif\\PycharmProjects\\Search_Engine\\Data\\date=07-08-2020'
        self.savedFileMainFolder = 'C:\\Users\\barif\\PycharmProjects\\Search_Engine'
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = False

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath
