import {
  Body,
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Post,
  Put,
  Query,
  UploadedFile,
  UseInterceptors,
  Delete,
} from '@nestjs/common';
import { ShoppingService } from './shopping.service';
import { FileInterceptor } from '@nestjs/platform-express';
import { diskStorage } from 'multer';
import { extname } from 'path';
import { CreateShoppingServiceDto } from './dto/create-shopping-service.dto';
import { ShoppingResultadoService } from './interface/shopping-resultado.interface';
import { TablaDatos } from './interface/tabla-datos.interface';

@Controller('shopping')
export class ShoppingController {
  constructor(private readonly shoppingService: ShoppingService) {}
  
  @Post('upload')
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: './uploads',
        filename: (req, file, callback) => {
          const uniqueSuffix =
            Date.now() + '-' + Math.round(Math.random() * 1e9);
          const ext = extname(file.originalname);
          const filename = `${uniqueSuffix}${ext}`;
          callback(null, filename);
        },
      }),
      fileFilter: (req, file, callback) => {
        const allowedExtensions = ['.xlsx', '.xls', '.csv'];
        const ext = extname(file.originalname).toLowerCase();

        if (!allowedExtensions.includes(ext)) {
          return callback(
            new Error('Solo se permiten archivos Excel y CSV'),
            false,
          );
        }

        callback(null, true);
      },
    }),
  )
  create(
    @Body() CreateShoppingServiceDto: CreateShoppingServiceDto,
    @UploadedFile() file: Express.Multer.File,
  ): Promise<ShoppingResultadoService> {
    return this.shoppingService.create(
      CreateShoppingServiceDto,
      file,
    );
  }

  @Get('tabla/datos_compras')
  async obtenerDatosCompras(
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 10,
    @Query('filtros') filtrosJson?: string,
  ): Promise<TablaDatos> {
    // Convertir los filtros de JSON a objeto si existe
    const filtros = filtrosJson ? JSON.parse(filtrosJson) : {};
    return this.shoppingService.obtenerDatosCompras(
      page,
      limit,
      filtros,
    );
  }

  @Get('tabla/:nombreTabla')
  async obtenerDatosTabla(
    @Param('nombreTabla') nombreTabla: string,
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 10,
    @Query('filtros') filtrosJson?: string,
  ): Promise<TablaDatos> {
    // Convertir los filtros de JSON a objeto si existe
    const filtros = filtrosJson ? JSON.parse(filtrosJson) : {};

    return this.shoppingService.obtenerDatosTabla(
      nombreTabla,
      page,
      limit,
      filtros,
      {
        aplicarLimpieza: true,
        columnasLimpiar: [
          'estado',
          'numero_orden',
          'proveedor',
          'producto',
          'categoria',
          'departamento',
        ],
      },
    );
  }

  //servicio para crear un nuevo registro
  @Post('tabla/:nombreTabla')
  async agregarRegistroTabla(
    @Param('nombreTabla') nombreTabla: string,
    @Body() datos: Record<string, any>,
  ): Promise<any> {
    return this.shoppingService.agregarRegistro(nombreTabla, datos);
  }

  @Put('tabla/:nombreTabla/:id')
  async actualizarRegistroTabla(
    @Param('nombreTabla') nombreTabla: string,
    @Param('id', ParseIntPipe) id: number,
    @Body() datos: Record<string, any>,
  ): Promise<{ mensaje: string }> {
    console.log('Recibido:', datos);
    return this.shoppingService.actualizarRegistro(nombreTabla, id, datos);
  }

  @Delete('tabla/:nombreTabla/:id')
  async eliminarRegistroTabla(
    @Param('nombreTabla') nombreTabla: string,
    @Param('id', ParseIntPipe) id: number,
  ): Promise<{ mensaje: string }> {
    return this.shoppingService.eliminarRegistro(nombreTabla, id);
  }

  @Get('tablas')
  async obtenerTablasSistema(): Promise<string[]> {
    return this.shoppingService.obtenerTablasSistema();
  }

  @Get('tabla/:nombreTabla/todos')
  async obtenerTodosLosRegistrosSinFiltros(
    @Param('nombreTabla') nombreTabla: string,
  ): Promise<any> {
    return this.shoppingService.obtenerTodosLosRegistrosSinFiltros(
      nombreTabla,
    );
  }
}