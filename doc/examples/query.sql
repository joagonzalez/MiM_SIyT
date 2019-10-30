select tel 
from anotado 
    inner join persona  on (persona.nombre = anotado.nombre and persona.apellido = anotado.apellido)
    inner joint empresa on (persona.empresa = empresa.nombre)
where materia = 'danza arabe'