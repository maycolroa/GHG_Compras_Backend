import {
  Injectable,
  Logger,
  BadRequestException,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, DataSource } from 'typeorm';
import { ShoppingEntity } from './entities/shopping.entity';
import { CreateShoppingServiceDto } from './dto/create-shopping-service.dto';
import { updateShoppingServiceDtO } from './dto/update-shopping-service.dto';
import * as fs from 'fs';
import * as path from 'path';
import * as XLSX from 'xlsx';
import * as csv from 'csv-parser';
import { ColumnaDefinicion } from './interface/columna-definicion.interface';
import { ShoppingResultadoService } from './interface/shopping-resultado.interface';
import { TablaDatos, ColumnaInfo } from './interface/tabla-datos.interface';

@Injectable()
export class ShoppingService {
  private readonly logger = new Logger('ShoppingService');

  constructor(
    @InjectRepository(ShoppingEntity)
    private readonly shoppingRepository: Repository<ShoppingEntity>,
    private readonly dataSource: DataSource,
  ) {}

  async create(
    createShoppingServiceDto: CreateShoppingServiceDto,
    file: Express.Multer.File,
  ): Promise<ShoppingResultadoService> {
    const { nombreTabla, usuarioId } = createShoppingServiceDto;

    try {
      // Validar que el nombre de la tabla sea válido para PostgreSQL
      this.validarNombreTabla(nombreTabla);

      // Verificar si ya existe una importación con el mismo nombre de archivo y tabla
      const importacionExistente = await this.shoppingRepository.findOne({
        where: {
          nombreArchivo: file.originalname,
          nombreTabla: nombreTabla,
          isActive: true,
        },
      });

      if (importacionExistente) {
        this.logger.log(
          `Ya existe una importación para el archivo ${file.originalname} en la tabla ${nombreTabla}`,
        );

        // Eliminar la importación anterior
        await this.remove(importacionExistente.id);
        this.logger.log(
          `Importación anterior eliminada. Creando nueva importación.`,
        );
      }

      // 1. Analizar la estructura del archivo (una sola vez)
      const { estructuraColumnas } = await this.analizarArchivo(file);
      let { datos } = await this.analizarArchivo(file);

      if (estructuraColumnas.length === 0) {
        throw new BadRequestException(
          'No se pudo determinar la estructura del archivo. Posiblemente está vacío o tiene un formato incorrecto.',
        );
      }

      // 2. Crear nueva entrada en la tabla de importaciones
      const nuevaImportacion = this.shoppingRepository.create({
        nombreArchivo: file.originalname,
        tipoArchivo: path.extname(file.originalname).substring(1),
        nombreTabla,
        rutaArchivo: file.path,
        estructuraColumnas,
        usuarioId,
      });

      await this.shoppingRepository.save(nuevaImportacion);

      // 3. Crear la tabla dinámicamente en la base de datos
      await this.crearTablaDinamica(nombreTabla, estructuraColumnas);

      // 4. Sanitizar valores largos si es necesario
      datos = this.sanitizarValoresLargos(datos, estructuraColumnas);

      // 5. Insertar los datos
      await this.insertarDatosEnTabla(nombreTabla, estructuraColumnas, datos);

      // 6. Actualizar el recuento de registros
      nuevaImportacion.totalRegistros = datos.length;
      await this.shoppingRepository.save(nuevaImportacion);

      return {
        id: nuevaImportacion.id,
        nombreTabla,
        totalRegistros: datos.length,
        mensaje: 'Archivo importado correctamente',
        estructuraColumnas,
      };
    } catch (error) {
      // Para depuración, imprimir más detalles del error
      this.logger.error(`Error detallado: ${JSON.stringify(error)}`);
      this.handleExceptions(error);
    }
  }

  private validarNombreTabla(nombreTabla: string): void {
    // Verificar que el nombre de la tabla cumple con las reglas de PostgreSQL
    const validRegex = /^[a-z][a-z0-9_]*$/;
    if (!validRegex.test(nombreTabla)) {
      throw new BadRequestException(
        'El nombre de la tabla debe comenzar con una letra y solo puede contener letras minúsculas, números y guiones bajos',
      );
    }

    // Verificar que no es una palabra reservada
    const palabrasReservadas = [
      'select',
      'from',
      'where',
      'insert',
      'update',
      'delete',
      'drop',
      'create',
      'table',
      'index',
      'view',
      'sequence',
      'trigger',
      'user',
      'group',
      'order',
      'by',
      'having',
      'limit',
      'offset',
      'join',
    ];

    if (palabrasReservadas.includes(nombreTabla.toLowerCase())) {
      throw new BadRequestException(
        `El nombre "${nombreTabla}" es una palabra reservada en SQL y no puede usarse como nombre de tabla`,
      );
    }
  }

  private async analizarArchivo(
    file: Express.Multer.File,
  ): Promise<{ estructuraColumnas: ColumnaDefinicion[]; datos: any[] }> {
    try {
      const extension = path.extname(file.originalname).toLowerCase();
      let datos: any[] = [];

      if (extension === '.xlsx' || extension === '.xls') {
        // Leer archivo Excel
        const workbook = XLSX.readFile(file.path, {
          cellDates: true, // Mantener las fechas como objetos Date
          dateNF: 'yyyy-mm-dd', // Formato de fecha
        });
        const sheet_name_list = workbook.SheetNames;
        datos = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
      } else if (extension === '.csv') {
        // Leer archivo CSV
        datos = await this.leerCSV(file.path);
      } else {
        throw new BadRequestException(
          `Formato de archivo no soportado: ${extension}`,
        );
      }

      if (datos.length === 0) {
        throw new BadRequestException('El archivo no contiene datos');
      }

      // Debug log for original columns
      this.logger.debug(
        `Columnas originales: ${JSON.stringify(Object.keys(datos[0]))}`,
      );

      // Normalizar nombres de columnas
      datos = this.normalizarNombresColumnas(datos);

      // More debug logging of normalized columns
      this.logger.debug(
        `Columnas normalizadas: ${JSON.stringify(Object.keys(datos[0]))}`,
      );

      // Determinar la estructura de columnas
      const estructuraColumnas = this.analizarEstructuraColumnas(datos);

      return { estructuraColumnas, datos };
    } catch (error) {
      this.logger.error(`Error al analizar archivo: ${error.message}`);
      throw new BadRequestException(
        'Error al analizar el archivo: ' + error.message,
      );
    }
  }

  private normalizarNombresColumnas(datos: any[]): any[] {
    if (datos.length === 0) return datos;

    const primerRegistro = datos[0];
    const columnas = Object.keys(primerRegistro);

    // If no columns need normalization, return the original data
    if (!columnas.some((col) => /[^a-zA-Z0-9_]/.test(col))) {
      return datos;
    }

    // Create a mapping from original names to normalized names
    const mapaNombres = {};
    for (const columna of columnas) {
      const nombreNormalizado = columna
        .replace(/[^a-zA-Z0-9_]/g, '_')
        .toLowerCase();
      mapaNombres[columna] = nombreNormalizado;

      // Debug log for column mapping
      this.logger.debug(
        `Mapeando columna: "${columna}" -> "${nombreNormalizado}"`,
      );
    }

    // Apply normalization to all records
    return datos.map((registro) => {
      const nuevoRegistro = {};
      for (const [nombreOriginal, valor] of Object.entries(registro)) {
        const nombreNormalizado = mapaNombres[nombreOriginal] || nombreOriginal;
        nuevoRegistro[nombreNormalizado] = valor;
      }
      return nuevoRegistro;
    });
  }

  private analizarEstructuraColumnas(datos: any[]): ColumnaDefinicion[] {
  if (datos.length === 0) return [];

  // Get all possible columns from all records
  const todasLasColumnas = new Set<string>();
  for (const registro of datos) {
    for (const columna of Object.keys(registro)) {
      todasLasColumnas.add(columna);
    }
  }

  // Analyze each column to determine its type
  const estructuraColumnas: ColumnaDefinicion[] = [];
  for (const columna of todasLasColumnas) {
    // Examine non-null values for this column
    const valoresNoNulos = datos
      .map((registro) => registro[columna])
      .filter((valor) => valor !== null && valor !== undefined);

    let tipo = 'text'; // Default to text for safety
    if (valoresNoNulos.length > 0) {
      // For numeric values
      if (typeof valoresNoNulos[0] === 'number') {
        // Check if any value has decimal points
        const hasDecimals = valoresNoNulos.some(
          (v) => typeof v === 'number' && Math.floor(v) !== v,
        );

        // Use appropriate type based on whether decimals exist
        tipo = hasDecimals ? 'decimal' : 'int';

        // Verify all values are numeric
        const todosSonNumeros = valoresNoNulos.every(
          (v) => typeof v === 'number',
        );
        if (!todosSonNumeros) {
          tipo = 'text'; // If there's a mix of types, use text
        }
      }
      // For dates
      else if (valoresNoNulos[0] instanceof Date) {
        tipo = 'date';

        // Check that all values are dates
        const todosSonFechas = valoresNoNulos.every((v) => v instanceof Date);
        if (!todosSonFechas) {
          tipo = 'text';
        }
      }
      // For booleans
      else if (typeof valoresNoNulos[0] === 'boolean') {
        tipo = 'boolean';

        // Check that all values are boolean
        const todosSonBooleanos = valoresNoNulos.every(
          (v) => typeof v === 'boolean',
        );
        if (!todosSonBooleanos) {
          tipo = 'text';
        }
      }
      // For strings that might contain numbers
      else if (typeof valoresNoNulos[0] === 'string') {
        // Comprobar si hay al menos un valor que claramente NO es un número
        const hayValorNoNumerico = valoresNoNulos.some(valor => {
          if (typeof valor !== 'string') return false;
          // Si contiene letras o caracteres especiales (excepto punto, coma o guion)
          return /[a-zA-Z]/.test(valor) || /[^0-9\.,\-\s]/.test(valor);
        });

        // Si hay al menos un valor no numérico, tratar como texto
        if (hayValorNoNumerico) {
          tipo = 'text';
          this.logger.debug(
            `Columna "${columna}" detectada como texto porque contiene valores no numéricos`,
          );
        } else {
          // Verificamos si podría ser un número
          const potentialNumbers = valoresNoNulos
            .map((v) => {
              if (typeof v !== 'string') return null;
              // Remove commas and clean up
              const cleaned = v.replace(/[^\d.-]/g, '');
              return !isNaN(Number(cleaned)) ? cleaned : null;
            })
            .filter((v) => v !== null);

          // Ser más conservador - solo considerar numérico si TODOS los valores parecen números
          if (potentialNumbers.length === valoresNoNulos.length) {
            // Check if any have decimal points
            const hasDecimals = potentialNumbers.some((v) => v.includes('.'));
            tipo = hasDecimals ? 'decimal' : 'int';

            this.logger.debug(
              `Columna "${columna}" detectada como numérica desde strings`,
            );
          } else {
            tipo = 'text';
            this.logger.debug(
              `Columna "${columna}" detectada como texto porque no todos los valores son numéricos`,
            );
          }
        }
      }
    }

    estructuraColumnas.push({
      nombre: columna,
      tipo,
      esNullable: true, // Allow null values by default
    });
  }

  return estructuraColumnas;
}

private determinarTipoDeDato(valor: any): string {
  if (valor === null || valor === undefined) {
      return 'varchar';
  }
  
  if (typeof valor === 'number') {
    // Determinar si es entero o decimal
    return Number.isInteger(valor) ? 'int' : 'decimal';
  }

  if (typeof valor === 'boolean') {
    return 'boolean';
  }

  if (typeof valor === 'string') {
    // Intentar determinar si es una fecha
    const posibleFecha = new Date(valor);
    if (
      !isNaN(posibleFecha.getTime()) &&
      (valor.includes('-') || valor.includes('/'))
    ) {
      return 'date';
    }

    // Si es una cadena que podría ser larga, usar text
    // Reducimos el límite a 200 para dar margen de seguridad
    if (valor.length > 200) {
      return 'text';
    }

    return 'varchar';
  }

  if (valor instanceof Date) {
    return 'date';
  }

  // Valor predeterminado
  return 'varchar';
}

  private async leerCSV(filePath: string): Promise<any[]> {
    return new Promise((resolve, reject) => {
      const results = [];
      fs.createReadStream(filePath)
        .pipe(
          csv({
            separator: this.detectarSeparadorCSV(filePath),
            skipLines: 0,
            strict: false, // Ser más tolerante con errores en el CSV
          }),
        )
        .on('data', (data) => results.push(data))
        .on('end', () => {
          resolve(results);
        })
        .on('error', (error) => {
          reject(error);
        });
    });
  }

  private detectarSeparadorCSV(filePath: string): string {
    // Lee un fragmento del archivo para detectar el separador
    try {
      const fragment = fs
        .readFileSync(filePath, { encoding: 'utf8', flag: 'r' })
        .slice(0, 1000);

      const separadores = [',', ';', '\t', '|'];
      let mejorSeparador = ','; // Separador predeterminado
      let maxOcurrencias = 0;

      for (const sep of separadores) {
        const ocurrencias = (fragment.match(new RegExp(sep, 'g')) || []).length;
        if (ocurrencias > maxOcurrencias) {
          maxOcurrencias = ocurrencias;
          mejorSeparador = sep;
        }
      }

      return mejorSeparador;
    } catch (error) {
      this.logger.warn(`Error detectando separador CSV: ${error.message}`);
      return ','; // Usar coma como valor predeterminado
    }
  }

  private async crearTablaDinamica(
    nombreTabla: string,
    columnas: ColumnaDefinicion[],
  ): Promise<void> {
    if (columnas.length === 0) {
      throw new BadRequestException('No se puede crear una tabla sin columnas');
    }

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // Verificar si la tabla ya existe y eliminarla si es necesario
      await queryRunner.query(`DROP TABLE IF EXISTS "${nombreTabla}"`);

      let createTableSql = `CREATE TABLE "${nombreTabla}" (
        "id" SERIAL PRIMARY KEY,`;

      columnas.forEach((columna, index) => {
        const tipoSQL = this.mapearTipoAPostgres(columna.tipo);

        createTableSql += `
        "${columna.nombre}" ${tipoSQL}${columna.esNullable ? '' : ' NOT NULL'}`;

        if (index < columnas.length - 1) {
          createTableSql += ',';
        }
      });

      createTableSql += `
      )`;

      await queryRunner.query(createTableSql);
      await queryRunner.commitTransaction();

      // Crear índices para mejorar el rendimiento de consultas comunes
      await this.crearIndices(nombreTabla, columnas);
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error al crear tabla: ${error.message}`);
      throw new InternalServerErrorException(
        'Error al crear la tabla en la base de datos: ' + error.message,
      );
    } finally {
      await queryRunner.release();
    }
  }

  private async crearIndices(
    nombreTabla: string,
    columnas: ColumnaDefinicion[],
  ): Promise<void> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();

    try {
      // Crear índices para columnas que probablemente se usarán en búsquedas
      for (const columna of columnas) {
        // Solo crear índices para tipos específicos que podrían beneficiarse
        if (['int', 'date', 'varchar'].includes(columna.tipo)) {
          const indexName = `idx_${nombreTabla}_${columna.nombre}`;
          try {
            // Usar índice btree para estos tipos de datos
            await queryRunner.query(
              `CREATE INDEX "${indexName}" ON "${nombreTabla}" USING btree ("${columna.nombre}")`,
            );
          } catch (error) {
            // Si falla la creación del índice, solo registrar el error pero continuar
            this.logger.warn(
              `Error al crear índice ${indexName}: ${error.message}`,
            );
          }
        }
      }
    } finally {
      await queryRunner.release();
    }
  }

  private mapearTipoAPostgres(tipo: string): string {
    const mapping = {
      int: 'NUMERIC(20,0)', // Changed from INTEGER/BIGINT to NUMERIC for all integers
      decimal: 'NUMERIC(20,4)', // Use NUMERIC with higher precision for decimals
      varchar: 'TEXT',
      text: 'TEXT',
      date: 'TIMESTAMP',
      timestamp: 'TIMESTAMP',
      boolean: 'BOOLEAN',
    };

    return mapping[tipo] || 'TEXT';
  }

  private async insertarDatosEnTabla(
    nombreTabla: string,
    columnas: ColumnaDefinicion[],
    datos: any[],
  ): Promise<void> {
    if (datos.length === 0) return;

    // Enhanced preprocessing of data - look for any decimal numbers
    const hasDecimalNumbers = datos.some((registro) => {
      return Object.values(registro).some(
        (valor) => typeof valor === 'number' && Math.floor(valor) !== valor,
      );
    });

    if (hasDecimalNumbers) {
      this.logger.warn(
        'Se han detectado valores decimales en los datos. Asegurándose de usar NUMERIC para campos numéricos.',
      );

      // Fix column types that might have decimals
      columnas = columnas.map((col) => {
        if (col.tipo === 'int') {
          const hasDecimals = datos.some((reg) => {
            const valor = reg[col.nombre];
            return typeof valor === 'number' && Math.floor(valor) !== valor;
          });

          if (hasDecimals) {
            this.logger.warn(
              `Columna "${col.nombre}" contiene valores decimales, cambiando tipo a 'decimal'`,
            );
            return { ...col, tipo: 'decimal' };
          }
        }
        return col;
      });
    }

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();

    try {
      // For better performance, use insertion by batches
      const tamanoLote = 100; // Smaller batch size to reduce the impact of failures
      let totalRecords = 0;

      for (let i = 0; i < datos.length; i += tamanoLote) {
        const lote = datos.slice(i, i + tamanoLote);

        // Start transaction for this batch
        await queryRunner.startTransaction();

        try {
          // Use the insertarLote method
          await this.insertarLote(queryRunner, nombreTabla, columnas, lote);
          await queryRunner.commitTransaction();
          totalRecords += lote.length;
          this.logger.log(
            `Transacción completada: ${lote.length} registros procesados (Total: ${totalRecords}/${datos.length})`,
          );
        } catch (error) {
          await queryRunner.rollbackTransaction();
          this.logger.error(
            `Error en un lote, se han perdido ${lote.length} registros: ${error.message}`,
          );
        }
      }

      if (totalRecords === 0) {
        throw new InternalServerErrorException(
          'Error al insertar datos: ningún registro fue insertado exitosamente',
        );
      } else if (totalRecords < datos.length) {
        this.logger.warn(
          `Inserción parcial: ${totalRecords} de ${datos.length} registros fueron insertados exitosamente`,
        );
      }
    } finally {
      await queryRunner.release();
    }
  }

  private async insertarLote(
    queryRunner: any,
    nombreTabla: string,
    columnas: ColumnaDefinicion[],
    lote: any[],
  ): Promise<void> {
    if (lote.length === 0) return;

    try {
      let recordsProcessed = 0;
      let recordsFailed = 0;

      // Process each record in the batch
      for (const registro of lote) {
        try {
          // Filter only columns with non-null values
          const columnasFiltradas = [];
          const valores = [];
          const columnasMap = new Map(columnas.map((col) => [col.nombre, col]));

          // Get all column names from the record
          for (const nombreColumna of Object.keys(registro)) {
            const valor = registro[nombreColumna];

            // Only include values that are not undefined or null
            if (valor !== undefined && valor !== null) {
              // Check if this column is defined in the table structure
              if (columnasMap.has(nombreColumna)) {
                columnasFiltradas.push(nombreColumna);

                // Sanitize values according to type
                const columnaInfo = columnasMap.get(nombreColumna);
                const valorSanitizado = this.sanitizarValorSegunTipo(
                  valor,
                  columnaInfo.tipo,
                );
                valores.push(valorSanitizado);
              } else {
                this.logger.warn(
                  `Columna "${nombreColumna}" no encontrada en la estructura de la tabla`,
                );
              }
            }
          }

          // Skip this record if there are no valid columns
          if (columnasFiltradas.length === 0) continue;

          // Create INSERT query with filtered columns
          let query = `INSERT INTO "${nombreTabla}" (`;
          query += columnasFiltradas.map((col) => `"${col}"`).join(', ');
          query += ') VALUES (';

          // Create placeholders for values
          const placeholders = columnasFiltradas
            .map((_, i) => `$${i + 1}`)
            .join(', ');
          query += placeholders;
          query += ')';

          // Execute the query for this record
          await queryRunner.query(query, valores);
          recordsProcessed++;
        } catch (error) {
          recordsFailed++;

          // Enhanced error logging with problematic values
          this.logger.error(`Error al insertar registro: ${error.message}`);

          // Log problematic values if it's a numeric error
          if (error.message.includes('syntax for type') && registro) {
            const problematicFields = Object.entries(registro)
              .filter((entry) => {
                const val = entry[1];
                return (
                  (typeof val === 'number' && !Number.isInteger(val)) ||
                  (typeof val === 'string' &&
                    !isNaN(parseFloat(val)) &&
                    val.includes('.'))
                );
              })
              .map((entry) => `${entry[0]}: ${entry[1]}`);

            if (problematicFields.length > 0) {
              this.logger.error(
                `Campos con valores problemáticos: ${problematicFields.join(', ')}`,
              );
            }
          }
        }
      }

      this.logger.log(
        `Lote procesado: ${recordsProcessed} registros insertados, ${recordsFailed} fallidos`,
      );
    } catch (error) {
      this.logger.error(`Error general en insertarLote: ${error.message}`);
      throw new InternalServerErrorException(
        'Error al insertar los datos en la tabla: ' + error.message,
      );
    }
  }

  private sanitizarValoresLargos(
    datos: any[],
    columnas: ColumnaDefinicion[],
  ): any[] {
    return datos.map((registro) => {
      const nuevoRegistro = { ...registro };

      // Check all columns for lengths
      for (const columna of columnas) {
        const valor = nuevoRegistro[columna.nombre];

        // If it's a string and the column is varchar, check length
        if (typeof valor === 'string' && columna.tipo === 'varchar') {
          // If exceeds 250 characters, truncate with a warning
          if (valor.length > 250) {
            this.logger.warn(
              `Valor truncado para columna ${columna.nombre} (${valor.length} caracteres)`,
            );
            nuevoRegistro[columna.nombre] = valor.substring(0, 250);
          }
        }
      }

      return nuevoRegistro;
    });
  }

  async findAll() {
    return this.shoppingRepository.find({
      where: { isActive: true },
      order: { createdAt: 'DESC' },
    });
  }

  async findOne(id: string) {
    const importacion = await this.shoppingRepository.findOne({
      where: { id, isActive: true },
    });

    if (!importacion) {
      throw new NotFoundException(`Importación con ID ${id} no encontrada`);
    }

    return importacion;
  }

  // Función que faltaba y generaba el error
  private async verificarTablaExiste(nombreTabla: string): Promise<boolean> {
    const queryRunner = this.dataSource.createQueryRunner();
    try {
      const result = await queryRunner.query(
        `
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = 'public' 
          AND table_name = $1
        )
        `,
        [nombreTabla],
      );

      return result[0].exists;
    } finally {
      await queryRunner.release();
    }
  }

  async obtenerDatosTabla(
    nombreTabla: string,
    page: number = 1,
    limit: number = 100,
    filtros: Record<string, any> = {},
    opcionesVisualizacion: {
      columnasOcultas?: string[];
      columnasLimpiar?: string[];
      aplicarLimpieza?: boolean;
    } = {},
  ): Promise<TablaDatos> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();

    try {
      // Verificar si la tabla existe
      const tablaExiste = await this.verificarTablaExiste(nombreTabla);
      if (!tablaExiste) {
        throw new NotFoundException(`La tabla "${nombreTabla}" no existe`);
      }

      // Obtener la estructura de la tabla
      const estructuraColumnas = await this.obtenerEstructuraTabla(nombreTabla);

      // Construir consulta con filtros
      let whereClause = '';
      const parametros = [];

      if (Object.keys(filtros).length > 0) {
        whereClause = ' WHERE ';
        const condiciones = [];

        let paramIndex = 1;
        for (const [campo, valor] of Object.entries(filtros)) {
          // Validar que el campo existe en la tabla
          if (estructuraColumnas.some((col) => col.nombre === campo)) {
            condiciones.push(`"${campo}" = $${paramIndex}`);
            parametros.push(valor);
            paramIndex++;
          }
        }

        if (condiciones.length > 0) {
          whereClause += condiciones.join(' AND ');
        } else {
          whereClause = '';
        }
      }

      // Obtener el total de registros con los filtros aplicados
      const countQuery = `SELECT COUNT(*) FROM "${nombreTabla}"${whereClause}`;
      const countResult = await queryRunner.query(countQuery, parametros);
      const totalRegistros = parseInt(countResult[0].count, 10);

      // Calcular offset para paginación
      const offset = (page - 1) * limit;

      // Obtener los datos de la tabla con paginación
      const query = `SELECT * FROM "${nombreTabla}"${whereClause} LIMIT ${limit} OFFSET ${offset}`;
      let datos = await queryRunner.query(query, parametros);

      // Aplicar limpieza de datos si se solicita
      if (opcionesVisualizacion.aplicarLimpieza) {
        datos = await this.limpiarDatosTabla(
          datos,
          opcionesVisualizacion.columnasOcultas || [],
          opcionesVisualizacion.columnasLimpiar || [],
        );
      }

      return {
        nombreTabla,
        columnas: estructuraColumnas,
        datos,
        totalRegistros,
        page,
        limit,
        totalPages: Math.ceil(totalRegistros / limit),
      };
    } catch (error) {
      if (error instanceof NotFoundException) {
        throw error;
      }
      this.logger.error(`Error al obtener datos: ${error.message}`);
      throw new InternalServerErrorException(
        'Error al obtener los datos de la tabla: ' + error.message,
      );
    } finally {
      await queryRunner.release();
    }
  }

  // funcion para llamar las tablas que existen
  async obtenerTablasSistema(): Promise<string[]> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();

    try {
      // Consulta para obtener las tablas públicas en PostgreSQL
      // excluyendo tablas del sistema y las tablas específicas que no quieres mostrar
      const result = await queryRunner.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        AND table_name NOT IN ('usuarios', 'ShoppingService')
        ORDER BY table_name
      `);

      return result.map((row) => row.table_name);
    } catch (error) {
      this.logger.error(`Error al obtener lista de tablas: ${error.message}`);
      throw new InternalServerErrorException(
        'Error al obtener la lista de tablas',
      );
    } finally {
      await queryRunner.release();
    }
  }

  private async obtenerEstructuraTabla(
    nombreTabla: string,
  ): Promise<ColumnaInfo[]> {
    const queryRunner = this.dataSource.createQueryRunner();
    try {
      const columnasInfo = await queryRunner.query(
        `
        SELECT column_name, data_type, 
              is_nullable = 'YES' as is_nullable,
              column_default
        FROM information_schema.columns 
        WHERE table_name = $1
        ORDER BY ordinal_position
        `,
        [nombreTabla],
      );

      return columnasInfo.map((col) => ({
        nombre: col.column_name,
        tipo: col.data_type,
        esNullable: col.is_nullable,
        valorPredeterminado: col.column_default,
      }));
    } finally {
      await queryRunner.release();
    }
  }

  /**
   * Método mejorado para actualizar un registro en una tabla
   */
  async actualizarRegistro(
    nombreTabla: string,
    id: number,
    datos: Record<string, any>,
  ): Promise<{ mensaje: string }> {
    this.logger.log(
      `Intentando actualizar registro ${id} en tabla ${nombreTabla}`,
    );
    this.logger.debug(`Datos recibidos: ${JSON.stringify(datos)}`);

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // Verificar si la tabla existe
      const tablaExiste = await this.verificarTablaExiste(nombreTabla);
      if (!tablaExiste) {
        throw new NotFoundException(`La tabla "${nombreTabla}" no existe`);
      }

      // Verificar si el registro existe
      const existeRegistro = await queryRunner.query(
        `SELECT EXISTS(SELECT 1 FROM "${nombreTabla}" WHERE id = $1)`,
        [id],
      );

      if (!existeRegistro[0].exists) {
        throw new NotFoundException(
          `No se encontró un registro con id ${id} en la tabla "${nombreTabla}"`,
        );
      }

      // Obtener la estructura de la tabla para validar los campos
      const estructura = await this.obtenerEstructuraTabla(nombreTabla);
      const camposValidos = estructura.map((col) => col.nombre);

      this.logger.debug(`Campos válidos en tabla: ${camposValidos.join(', ')}`);

      // Filtrar datos para incluir solo campos válidos
      const datosFiltrados = {};
      for (const [campo, valor] of Object.entries(datos)) {
        if (camposValidos.includes(campo) && campo !== 'id') {
          // Sanitizar el valor según el tipo de columna
          const columnaInfo = estructura.find((col) => col.nombre === campo);

          if (columnaInfo) {
            const valorSanitizado = this.sanitizarValorSegunTipo(
              valor,
              columnaInfo.tipo,
            );

            datosFiltrados[campo] = valorSanitizado;
            this.logger.debug(
              `Campo ${campo}: valor "${valor}" sanitizado a "${valorSanitizado}"`,
            );
          }
        }
      }

      if (Object.keys(datosFiltrados).length === 0) {
        throw new BadRequestException(
          'No se proporcionaron campos válidos para actualizar',
        );
      }

      // Construir la consulta de actualización
      let updateQuery = `UPDATE "${nombreTabla}" SET `;
      const setClauses = [];
      const parametros = [];
      let paramIndex = 1;

      for (const [campo, valor] of Object.entries(datosFiltrados)) {
        setClauses.push(`"${campo}" = $${paramIndex}`);
        parametros.push(valor);
        paramIndex++;
      }

      updateQuery += setClauses.join(', ');
      updateQuery += ` WHERE id = $${paramIndex}`;
      parametros.push(id);

      this.logger.debug(`Ejecutando query: ${updateQuery}`);
      this.logger.debug(`Con parámetros: ${JSON.stringify(parametros)}`);

      // Ejecutar la consulta de actualización
      const result = await queryRunner.query(updateQuery, parametros);

      // Verificar que se actualizó correctamente
      if (result && result[1] === 0) {
        throw new NotFoundException(
          `No se pudo actualizar el registro con id ${id} - no se encontró o no hay cambios`,
        );
      }

      await queryRunner.commitTransaction();

      this.logger.log(
        `Registro ${id} actualizado correctamente en tabla ${nombreTabla}`,
      );
      return { mensaje: 'Registro actualizado correctamente' };
    } catch (error) {
      await queryRunner.rollbackTransaction();

      // Mejorar el registro de errores
      this.logger.error(
        `Error al actualizar registro ${id} en tabla ${nombreTabla}: ${error.message}`,
      );
      if (error.stack) {
        this.logger.debug(`Stack de error: ${error.stack}`);
      }

      // Rethrow para mantener el tipo de error original
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException
      ) {
        throw error;
      }

      throw new InternalServerErrorException(
        `Error al actualizar el registro: ${error.message}`,
      );
    } finally {
      await queryRunner.release();
    }
  }

  // Función para limpiar datos
  async limpiarDatosTabla(
    datos: any[],
    columnasOcultas: string[] = [],
    columnasLimpiar: string[] = [],
  ): Promise<any[]> {
    this.logger.log('Iniciando limpieza de datos para visualización');

    if (!datos || datos.length === 0) {
      return datos;
    }

    return datos.map((registro) => {
      const registroLimpio = { ...registro };

      // Recorrer todas las propiedades del registro
      for (const [clave, valor] of Object.entries(registroLimpio)) {
        // Ocultar columnas especificadas reemplazándolas con null
        if (columnasOcultas.includes(clave)) {
          registroLimpio[clave] = null;
          continue;
        }

        // Eliminar espacios en blanco en las columnas especificadas
        if (columnasLimpiar.includes(clave) && typeof valor === 'string') {
          registroLimpio[clave] = valor.trim();
        }

        // Normalizar valores nulos o undefined
        if (valor === undefined || valor === '') {
          registroLimpio[clave] = null;
        }

        // Convertir formatos de fecha a un formato estándar si es una fecha válida
        if (
          typeof valor === 'string' &&
          (valor.includes('-') || valor.includes('/')) &&
          !isNaN(Date.parse(valor))
        ) {
          try {
            const fecha = new Date(valor);
            if (fecha instanceof Date && !isNaN(fecha.getTime())) {
              registroLimpio[clave] = fecha.toISOString().split('T')[0]; // Formato YYYY-MM-DD
            }
          } catch (e) {
            this.logger.warn(`Error al convertir fecha: ${e.message}`);
            // Si no se puede convertir, mantener el valor original
          }
        }
      }

      return registroLimpio;
    });
  }

  async exportarExcel(nombreTabla: string): Promise<Buffer> {
    try {
      // Obtener los datos completos de la tabla
      const { datos } = await this.obtenerDatosTabla(
        nombreTabla,
        1,
        Number.MAX_SAFE_INTEGER,
      );

      // Crear un nuevo libro de Excel
      const workbook = XLSX.utils.book_new();

      // Convertir los datos a worksheet
      const worksheet = XLSX.utils.json_to_sheet(datos);

      // Añadir la hoja al libro
      XLSX.utils.book_append_sheet(workbook, worksheet, nombreTabla);

      // Generar el buffer del archivo Excel
      return XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
    } catch (error) {
      this.logger.error(`Error al exportar a Excel: ${error.message}`);
      throw new InternalServerErrorException(
        'Error al exportar los datos a Excel: ' + error.message,
      );
    }
  }

  async update(
    id: string,
    updateShoppingServiceDto: updateShoppingServiceDtO,
  ) {
    const importacion = await this.findOne(id);

    try {
      // Actualizamos solo los campos permitidos
      if (updateShoppingServiceDto.nombreTabla) {
        importacion.nombreTabla = updateShoppingServiceDto.nombreTabla;
      }

      if (updateShoppingServiceDto.usuarioId) {
        importacion.usuarioId = updateShoppingServiceDto.usuarioId;
      }

      return this.shoppingRepository.save(importacion);
    } catch (error) {
      this.logger.error(`Error al actualizar: ${error.message}`);
      throw new InternalServerErrorException(
        'Error al actualizar la importación: ' + error.message,
      );
    }
  }

  /**
   * Función especializada para limpiar y normalizar datos de compras
   * Personalizada para la tabla datos_compras con reglas específicas
   * @param datos Datos de compras a limpiar
   * @returns Datos de compras limpios y normalizados
   */
  async limpiarDatosCompras(datos: any[]): Promise<any[]> {
    if (!datos || datos.length === 0) {
      return datos;
    }

    this.logger.log(
      `Limpiando ${datos.length} registros de compras`,
    );
    if (datos.length > 0) {
      this.logger.log(
        `IDs antes de limpieza: ${datos
          .slice(0, 10)
          .map((d) => d.id)
          .join(', ')}`,
      );
    }

    // Columnas a limpiar específicas para compras
    const columnasLimpiar = [
      'estado',
      'numero_orden',
      'proveedor',
      'producto',
      'categoria',
      'departamento'
    ];

    // Columnas a ocultar específicas para compras (si hay)
    const columnasOcultar = [
      'notas_internas',
      'comentarios_privados',
      'id_referencia'
    ];

    const datosLimpios = datos.map((compra) => {
      const compraLimpia = { ...compra };

      // IMPORTANTE: Preservar siempre el ID original
      compraLimpia.id = compra.id;

      // Aplicar limpieza básica
      for (const columna of columnasLimpiar) {
        if (
          compraLimpia[columna] &&
          typeof compraLimpia[columna] === 'string'
        ) {
          compraLimpia[columna] = compraLimpia[columna].trim();
        }
      }

      // Ocultar columnas especificadas
      for (const columna of columnasOcultar) {
        if (columna in compraLimpia) {
          compraLimpia[columna] = null;
        }
      }

      // Normalizar estados (convertir a formato estándar) pero NO filtrar
      if (compraLimpia.estado) {
        const estadoLower = String(compraLimpia.estado).toLowerCase();

        // Mantener el estado original y añadir clasificación
        if (
          estadoLower.includes('pagado') ||
          estadoLower.includes('finalizado') ||
          estadoLower.includes('completado') ||
          estadoLower.includes('entregado')
        ) {
          // Preservar el estado original para registros específicos
          // pero añadir la clasificación
          compraLimpia.estado_clasificacion = 'completado';
        } else if (
          estadoLower.includes('pendiente') ||
          estadoLower.includes('proceso') ||
          estadoLower.includes('autorización') ||
          estadoLower.includes('enviado')
        ) {
          // Preservar el estado original para registros específicos
          // pero añadir la clasificación
          compraLimpia.estado_clasificacion = 'pendiente';
        } else if (
          estadoLower.includes('cancel') ||
          estadoLower.includes('rechaz')
        ) {
          // Preservar el estado original para registros específicos
          // pero añadir la clasificación
          compraLimpia.estado_clasificacion = 'cancelado';
        } else {
          // Estado no reconocido, mantener original
          compraLimpia.estado_clasificacion = 'otro';
        }
      }

      // Normalizar valores monetarios (asegurar que sean numéricos)
      if ('valor_total' in compraLimpia) {
        if (
          compraLimpia.valor_total === null ||
          compraLimpia.valor_total === undefined ||
          compraLimpia.valor_total === ''
        ) {
          compraLimpia.valor_total = 0;
        } else if (typeof compraLimpia.valor_total === 'string') {
          // Eliminar caracteres no numéricos excepto punto decimal
          const valorLimpio = compraLimpia.valor_total.replace(
            /[^0-9.]/g,
            '',
          );
          compraLimpia.valor_total = parseFloat(valorLimpio) || 0;
        }
      }

      // Asegurar formato de fecha correcto
      const camposFecha = ['fecha_orden', 'fecha_entrega', 'fecha_pago'];
      for (const campoFecha of camposFecha) {
        if (compraLimpia[campoFecha] && typeof compraLimpia[campoFecha] === 'string') {
          try {
            const fecha = new Date(compraLimpia[campoFecha]);
            if (!isNaN(fecha.getTime())) {
              compraLimpia[campoFecha] = fecha.toISOString().split('T')[0];
              
              // Si es la fecha de orden, extraer el año para facilitar filtrado
              if (campoFecha === 'fecha_orden') {
                compraLimpia.anio = fecha.getFullYear();
              }
            }
          } catch (e) {
            this.logger.warn(`Error al convertir fecha ${campoFecha}: ${e.message}`);
            // Si no se puede convertir, mantener el valor original
          }
        }
      }

      return compraLimpia;
    });

    this.logger.log(`Registros después de limpieza: ${datosLimpios.length}`);
    if (datosLimpios.length > 0) {
      this.logger.log(
        `IDs después de limpieza: ${datosLimpios
          .slice(0, 10)
          .map((d) => d.id)
          .join(', ')}`,
      );
    }

    return datosLimpios;
  }

  /**
   * Método abreviado para obtener datos de compras ya limpios
   * @param page Número de página
   * @param limit Límite de registros por página
   * @param filtros Filtros a aplicar
   * @returns Datos de compras limpios
   */
  async obtenerDatosCompras(
    page: number = 1,
    limit: number = 10,
    filtros: Record<string, any> = {},
  ): Promise<TablaDatos> {
    // Obtener datos crudos
    const datosTabla = await this.obtenerDatosTabla(
      'datos_compras',  // Nombre de la tabla en la base de datos
      page,
      limit,
      filtros,
    );

    // Limpiar datos específicamente para compras
    datosTabla.datos = await this.limpiarDatosCompras(
      datosTabla.datos,
    );

    return datosTabla;
  }

  async eliminarRegistro(
    nombreTabla: string,
    id: number,
  ): Promise<{ mensaje: string }> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // Verificar si la tabla existe
      const tablaExiste = await this.verificarTablaExiste(nombreTabla);
      if (!tablaExiste) {
        throw new NotFoundException(`La tabla "${nombreTabla}" no existe`);
      }

      // Verificar si el registro existe
      const existeQuery = `SELECT EXISTS(SELECT 1 FROM "${nombreTabla}" WHERE id = $1)`;
      const existeResult = await queryRunner.query(existeQuery, [id]);

      if (!existeResult[0].exists) {
        throw new NotFoundException(
          `El registro con ID ${id} no existe en la tabla "${nombreTabla}"`,
        );
      }

      // Eliminar el registro
      const deleteQuery = `DELETE FROM "${nombreTabla}" WHERE id = $1`;
      await queryRunner.query(deleteQuery, [id]);

      await queryRunner.commitTransaction();

      return { mensaje: 'Registro eliminado correctamente' };
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error al eliminar registro: ${error.message}`);

      if (error instanceof NotFoundException) {
        throw error;
      }

      throw new InternalServerErrorException(
        'Error al eliminar el registro: ' + error.message,
      );
    } finally {
      await queryRunner.release();
    }
  }
  
  //obtener todos los datos sin filtros
  async obtenerTodosLosRegistrosSinFiltros(nombreTabla: string): Promise<any> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();

    try {
      // Verificar si la tabla existe
      const tablaExiste = await this.verificarTablaExiste(nombreTabla);
      if (!tablaExiste) {
        throw new NotFoundException(`La tabla "${nombreTabla}" no existe`);
      }

      // Consulta directa sin filtros, sin limpieza y sin paginación
      const query = `SELECT * FROM "${nombreTabla}" ORDER BY id ASC`;
      const datos = await queryRunner.query(query);

      // Obtener la estructura de la tabla
      const columnas = await this.obtenerEstructuraTabla(nombreTabla);

      this.logger.log(
        `Obtenidos ${datos.length} registros de la tabla ${nombreTabla} sin filtros ni limpieza`,
      );

      if (datos.length > 0) {
        this.logger.log(
          `IDs de los primeros 10 registros: ${datos
            .slice(0, 10)
            .map((d) => d.id)
            .join(', ')}`,
        );
      }

      return {
        nombreTabla,
        columnas,
        datos,
        totalRegistros: datos.length,
        mensaje: 'Datos obtenidos sin filtros ni limpieza',
      };
    } catch (error) {
      if (error instanceof NotFoundException) {
        throw error;
      }
      this.logger.error(
        `Error al obtener todos los registros: ${error.message}`,
      );
      throw new InternalServerErrorException(
        'Error al obtener todos los registros: ' + error.message,
      );
    } finally {
      await queryRunner.release();
    }
  }

  async remove(id: string) {
    const importacion = await this.findOne(id);

    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // Eliminar la tabla asociada
      await queryRunner.query(
        `DROP TABLE IF EXISTS "${importacion.nombreTabla}"`,
      );

      // Marcar como inactivo en lugar de eliminar físicamente
      importacion.isActive = false;
      await this.shoppingRepository.save(importacion);

      // Eliminar el archivo físico si existe
      if (importacion.rutaArchivo && fs.existsSync(importacion.rutaArchivo)) {
        fs.unlinkSync(importacion.rutaArchivo);
      }

      await queryRunner.commitTransaction();

      return { mensaje: 'Importación eliminada correctamente' };
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error al eliminar: ${error.message}`);
      throw new InternalServerErrorException(
        'Error al eliminar la importación: ' + error.message,
      );
    } finally {
      await queryRunner.release();
    }
  }

  async obtenerEstadisticas(nombreTabla: string): Promise<any> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();

    try {
      // Verificar si la tabla existe
      const tablaExiste = await this.verificarTablaExiste(nombreTabla);
      if (!tablaExiste) {
        throw new NotFoundException(`La tabla "${nombreTabla}" no existe`);
      }

      // Obtener la estructura de la tabla
      const columnasInfo = await this.obtenerEstructuraTabla(nombreTabla);

      // Obtener el total de filas
      const countResult = await queryRunner.query(
        `SELECT COUNT(*) FROM "${nombreTabla}"`,
      );
      const totalRows = parseInt(countResult[0].count, 10);

      // Analizar columnas según su tipo
      const columnsStats = {
        numericas: [],
        categoricas: [],
      };

      for (const columna of columnasInfo) {
        // Omitir la columna id para las estadísticas
        if (columna.nombre === 'id') continue;

        const tipoSQL = columna.tipo.toLowerCase();

        // Estadísticas para columnas numéricas
        if (
          tipoSQL.includes('int') ||
          tipoSQL.includes('numeric') ||
          tipoSQL.includes('decimal') ||
          tipoSQL.includes('float')
        ) {
          const stats = await queryRunner.query(`
            SELECT 
              MIN("${columna.nombre}") as min,
              MAX("${columna.nombre}") as max,
              AVG("${columna.nombre}") as avg,
              SUM("${columna.nombre}") as sum,
              COUNT(*) - COUNT("${columna.nombre}") as null_count,
              COUNT(DISTINCT "${columna.nombre}") as distinct_count
            FROM "${nombreTabla}"
          `);

          columnsStats.numericas.push({
            columna: columna.nombre,
            min: parseFloat(stats[0].min) || 0,
            max: parseFloat(stats[0].max) || 0,
            avg: parseFloat(stats[0].avg) || 0,
            sum: parseFloat(stats[0].sum) || 0,
            nullCount: parseInt(stats[0].null_count, 10),
            distinctCount: parseInt(stats[0].distinct_count, 10),
          });
        }
        // Estadísticas para columnas categóricas
        else {
          const nullCount = await queryRunner.query(`
            SELECT COUNT(*) - COUNT("${columna.nombre}") as null_count
            FROM "${nombreTabla}"
          `);

          const distinctCount = await queryRunner.query(`
            SELECT COUNT(DISTINCT "${columna.nombre}") as distinct_count
            FROM "${nombreTabla}"
          `);

          // Obtener los 10 valores más frecuentes
          const topValues = await queryRunner.query(`
            SELECT "${columna.nombre}" as value, COUNT(*) as count
            FROM "${nombreTabla}"
            WHERE "${columna.nombre}" IS NOT NULL
            GROUP BY "${columna.nombre}"
            ORDER BY count DESC
            LIMIT 10
          `);

          columnsStats.categoricas.push({
            columna: columna.nombre,
            topValues: topValues,
            nullCount: parseInt(nullCount[0].null_count, 10),
            distinctCount: parseInt(distinctCount[0].distinct_count, 10),
          });
        }
      }

      return {
        totalRows,
        columnas: columnasInfo,
        ...columnsStats,
      };
    } catch (error) {
      if (error instanceof NotFoundException) {
        throw error;
      }
      this.logger.error(`Error al obtener estadísticas: ${error.message}`);
      throw new InternalServerErrorException(
        'Error al obtener las estadísticas de la tabla: ' + error.message,
      );
    } finally {
      await queryRunner.release();
    }
  }

  async agregarRegistro(
    nombreTabla: string,
    datos: Record<string, any>,
  ): Promise<any> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // Verificar si la tabla existe
      const tablaExiste = await this.verificarTablaExiste(nombreTabla);
      if (!tablaExiste) {
        throw new NotFoundException(`La tabla "${nombreTabla}" no existe`);
      }

      // Obtener la estructura de la tabla para validar los campos
      const estructura = await this.obtenerEstructuraTabla(nombreTabla);
      const camposValidos = estructura
        .map((col) => col.nombre)
        .filter((nombre) => nombre !== 'id');

      // Filtrar datos para incluir solo campos válidos
      const datosFiltrados = {};
      for (const [campo, valor] of Object.entries(datos)) {
        if (camposValidos.includes(campo)) {
          const tipoColumna =
            estructura.find((col) => col.nombre === campo)?.tipo || 'varchar';
          datosFiltrados[campo] = this.sanitizarValorSegunTipo(
            valor,
            tipoColumna,
          );
        }
      }

      if (Object.keys(datosFiltrados).length === 0) {
        throw new BadRequestException(
          'No se proporcionaron campos válidos para insertar',
        );
      }

      // Construir la consulta de inserción
      let insertQuery = `INSERT INTO "${nombreTabla}" (`;
      const campos = Object.keys(datosFiltrados).map((campo) => `"${campo}"`);
      insertQuery += campos.join(', ');
      insertQuery += ') VALUES (';

      const placeholders = [];
      const valores = [];

      for (let i = 0; i < campos.length; i++) {
        placeholders.push(`$${i + 1}`);
        valores.push(datosFiltrados[campos[i].replace(/"/g, '')]);
      }

      insertQuery += placeholders.join(', ');
      insertQuery += ') RETURNING id';

      const result = await queryRunner.query(insertQuery, valores);
      await queryRunner.commitTransaction();

      return {
        id: result[0].id,
        mensaje: 'Registro agregado correctamente',
      };
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Error al agregar registro: ${error.message}`);

      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException
      ) {
        throw error;
      }

      throw new InternalServerErrorException(
        'Error al agregar el registro: ' + error.message,
      );
    } finally {
      await queryRunner.release();
    }
  }

  private sanitizarValorSegunTipo(valor: any, tipo: string): any {
    if (valor === null || valor === undefined) {
      return null;
    }

    switch (tipo) {
      case 'date':
      case 'timestamp':
        if (typeof valor === 'string') {
          // Try to convert to date
          const fecha = new Date(valor);
          if (!isNaN(fecha.getTime())) {
            return fecha;
          }
        }
        if (valor instanceof Date) {
          return valor;
        }
        return null;

      case 'int':
        if (typeof valor === 'string') {
          // Handle numbers in string format, remove any non-numeric chars except minus
          const numStr = valor.replace(/[^\d.-]/g, '');
          // For integers, parse and round
          const num = parseFloat(numStr);
          return isNaN(num) ? null : Math.round(num);
        }
        if (typeof valor === 'number') {
          return Math.round(valor); // Round to closest integer
        }
        return null;

      case 'decimal':
        if (typeof valor === 'string') {
          // Convert commas to dots to correctly parse
          const numStr = valor.replace(/,/g, '.');
          // Clean up any non-numeric characters except dots and minus
          const cleanStr = numStr.replace(/[^\d.-]/g, '');
          const num = parseFloat(cleanStr);
          return isNaN(num) ? null : num;
        }
        if (typeof valor === 'number') {
          return valor;
        }
        return null;

      case 'boolean':
        if (typeof valor === 'string') {
          const lowerVal = valor.toLowerCase();
          if (['true', 'yes', 'si', 's', 'y', '1'].includes(lowerVal)) {
            return true;
          }
          if (['false', 'no', 'n', '0'].includes(lowerVal)) {
            return false;
          }
          return null;
        }
        if (typeof valor === 'number') {
          return valor !== 0;
        }
        if (typeof valor === 'boolean') {
          return valor;
        }
        return null;

      default:
        if (typeof valor === 'object') {
          try {
            return JSON.stringify(valor);
          } catch {
            return String(valor);
          }
        }
        return String(valor);
    }
  }

  private handleExceptions(error: any) {
    this.logger.error(error);

    if (
      error instanceof BadRequestException ||
      error instanceof NotFoundException
    ) {
      throw error;
    }

    throw new InternalServerErrorException(
      'Error inesperado, revisa los logs del servidor',
    );
  }
}