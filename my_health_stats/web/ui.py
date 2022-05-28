from pywebio.input import input, FLOAT
from pywebio.output import put_text
from my_health_stats.orchestrator.generic import Orchestrator


def main_ui():
    o = Orchestrator()
    args = o.get_extract_parameters()
    ...

    # height = input("Input your height(cm)：", type=FLOAT)
    # weight = input("Input your weight(kg)：", type=FLOAT)
    #
    # BMI = weight / (height / 100) ** 2
    #
    # top_status = [(16, 'Severely underweight'), (18.5, 'Underweight'),
    #               (25, 'Normal'), (30, 'Overweight'),
    #               (35, 'Moderately obese'), (float('inf'), 'Severely obese')]
    #
    # for top, status in top_status:
    #     if BMI <= top:
    #         put_text('Your BMI: %.1f. Category: %s' % (BMI, status))
    #         break