FROM ubuntu:latest
USER root

COPY requirements.txt ./

RUN pip3 -q install pip --upgrade
RUN pip3 install --no-cache-dir -r requirements.txt


USER $NB_UID

CMD ["jupyter", "notebook", "--port=8888" ,"--no-browser", "--ip=0.0.0.0", "--allow-root"]