import csv


def load_comunidades_provincias_map (comunidades_file):

    provincia = {}

    try:  # Read table - comunidad_autonoma;provincia
        with open(comunidades_file) as f:
            reader = csv.reader(f, delimiter=';', quotechar='"', doublequote=False)
            reader.next()
        for line in reader:
            provincia[(line[2], line[3], line[0])] = line[1]
    except:
        pass

    return provincia


class Parse_comunidad_contratos_mapper:

    def __init__(self):
        self.comunidad = load_comunidades_provincias_map('./comunidades_y_provincias.csv')


    def __call__(self, key, value):
        try:
            contratos_hombres = 0
            contratos_mujeres = 0
            codigo_mes, provincia, municipio, total_contratos, contratos_hombres, contratos_mujeres = value.split('|')
            yield (provincia), (contratos_hombres, contratos_mujeres)
        except:
            pass


def join_comunidad_contratos_reduce(key, values):
    num_contratos_hombres = 0
    num_contratos_mujeres = 0

    comunidad_autonoma = key[:]

    for v in values:
        contratos_hombres, contratos_mujeres = v[:]
        num_contratos_hombres += int(contratos_hombres)
        num_contratos_mujeres += int(contratos_mujeres)

    yield comunidad_autonoma, (num_contratos_hombres, num_contratos_mujeres)

from dumbo import main

def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]

    o1 = job.additer(Parse_comunidad_contratos_mapper, join_comunidad_contratos_reduce, opts=inout_opts)

    if __name__ == "__main__":     main(runner)