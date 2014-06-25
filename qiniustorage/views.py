from rest_framework.generics import GenericAPIView
from rest_framework import status
from rest_framework.response import Response
from django.conf import settings
from django.db.models.fields.files import FieldFile
import os, logging

from qiniu.rs import PutPolicy
from qiniu import conf

from .backends import QiniuStorage

class AsyncPutView(GenericAPIView):
    model = None
    target_model = None
    target_field = None
    target_field_class = FieldFile
    storage_class = QiniuStorage 
    base_path = "" # No left strip since qiniu will have problems processing thumbnail with root slash like this /image.jpg
    bucket_name = None
    expires = 3600

    fsize_limit = None
    mime_limit = None

    def get_base_path(self):
        return self.base_path

    def get_upload_key(self, filename):
        return self.storage_class()._normalize_name(os.path.join(self.get_base_path(), filename))

    def get(self, request, *args, **kwargs):
        filename = kwargs.pop('filename')
        bucket_name = self.bucket_name and self.bucket_name or settings.QINIU_BUCKET_NAME
        key = self.get_upload_key(filename)
        pp = PutPolicy("%s:%s" % (bucket_name, key))
        pp.expires = self.expires
        pp.fsizeLimit = self.fsize_limit
        pp.mimeLimit = self.mime_limit
        conf.ACCESS_KEY = settings.QINIU_ACCESS_KEY
        conf.SECRET_KEY = settings.QINIU_SECRET_KEY
        return Response({
            'token': pp.token(),
            'key': key,
            'url': 'http://%s' % conf.UP_HOST
        }, status = status.HTTP_200_OK)
    
    def post(self, request, *args, **kwargs):
        tgt = self.target_model()
        field = None
        for f in self.target_model._meta.fields:
            if f.name == self.target_field:
                field = f
                break

        if field is None:
            raise Exception("Unable to find %s field on %s model" % (self.target_field, self.target_model))

        fieldfile = self.target_field_class(tgt, field, request.DATA['key'])
        fieldfile.storage = QiniuStorage() 
        setattr(tgt, self.target_field, fieldfile)
        try:
            self.pre_save(tgt)
            tgt.save()
            self.post_save(tgt, True)
        except Exception, e:
            logging.error('error',exc_info=e)
            
        return Response({
            'key': request.DATA['key'],
            'pk': tgt.pk
        }, status = status.HTTP_201_CREATED)

        
